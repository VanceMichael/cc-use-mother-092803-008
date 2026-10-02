"""赛事报名应用服务：命令编排、幂等、通知去重与争议查询。

写入路径严格按以下顺序执行（全程持锁）：

    读事件流 → fold 当前状态 → decide 纯决策 → 单事务追加事件
    + 写入通知去重 + 写入命令幂等响应

任何一步的领域校验失败都不会产生事件；通知重放命中去重表时直接返回原结果，
状态零变更。
"""
from __future__ import annotations

import secrets
from threading import RLock
from typing import Any, Optional

from .contracts import Request, Result, validate_request
from .domain import (
    ALL_GROUPS,
    DomainError,
    Command,
    decide,
    entry_history,
    fold,
    race_summary,
    slot_history,
)
from .store import EventStore, utc_now

WRITE_ACTIONS = {
    "create_race", "add_group", "authorize_staff",
    "open_registration", "close_registration",
    "register", "release_quota",
    "run_lottery", "publish_lottery",
    "payment_notice", "refund_notice",
    "request_withdrawal", "complete_withdrawal",
}
NOTICE_ACTIONS = {"payment_notice", "refund_notice"}


class MarathonEntryService:
    def __init__(self, store: Optional[EventStore] = None) -> None:
        self._owns_store = store is None
        self._store = store or EventStore(":memory:")
        self._lock = RLock()

    def close(self) -> None:
        if self._owns_store:
            self._store.close()

    # ---------- 命令入口 ----------

    def handle(self, request: Request) -> Result:
        validate_request(request)
        cached = self._store.get_response(request.request_id)
        if cached is not None:
            return Result(cached["accepted"], cached["state"], cached["message"], cached["data"])

        with self._lock:
            cached = self._store.get_response(request.request_id)
            if cached is not None:
                return Result(cached["accepted"], cached["state"], cached["message"], cached["data"])

            if request.action in WRITE_ACTIONS:
                return self._handle_write(request)
            try:
                result = self._handle_query(request)
            except DomainError as exc:
                result = Result(False, exc.state, exc.message, exc.data)
            # 查询结果同样按 request_id 固化，重放得到一致响应。
            self._store.save_response(request.request_id, result.as_dict())
            return result

    # ---------- 写入 ----------

    def _handle_write(self, request: Request) -> Result:
        stream_id = str(request.payload.get("race_id", "")).strip()
        if not stream_id:
            result = Result(False, "rejected", "缺少 race_id", {})
            self._store.save_response(request.request_id, result.as_dict())
            return result

        events = self._store.load_events(stream_id)
        view = fold(events)

        # 支付/退款通知按通道通知号去重：重放一律不再作用于状态。
        if request.action in NOTICE_ACTIONS:
            dup = self._duplicate_notice_result(request, events, view)
            if dup is not None:
                self._store.save_response(request.request_id, dup.as_dict())
                return dup

        cmd = Command(request.actor, request.action, request.payload)
        try:
            new_events = decide(view, cmd, request.created_at.isoformat())
        except DomainError as exc:
            result = Result(False, exc.state, exc.message, exc.data)
            self._store.save_response(request.request_id, result.as_dict())
            return result

        occurred_at = utc_now()
        specs: list[tuple[str, dict[str, Any], str, str]] = []
        notices: list[tuple[str, str]] = []
        public_events: list[dict[str, Any]] = []
        for item in new_events:
            event_id = "evt_" + secrets.token_hex(8)
            specs.append((item["event_type"], item["data"], event_id, occurred_at))
            for n in item.get("notices", []):
                notices.append(n)
            public_events.append({
                "event_id": event_id,
                "event_type": item["event_type"],
                "data": item["data"],
            })

        # 事件、通知去重、命令响应同一事务提交，三者同生共死。
        self._store.commit(
            stream_id=stream_id,
            expected_stream_seq=len(events),
            event_specs=specs,
            notices=notices,
            request_id=request.request_id,
            result=Result(
                True,
                view.phase,
                _success_message(request.action),
                {"events": public_events},
            ).as_dict(),
        )
        # 从提交结果回读即可，无需重新加载。
        return Result(True, view.phase, _success_message(request.action),
                      {"events": public_events})

    def _duplicate_notice_result(
        self, request: Request, events: list[dict[str, Any]], view
    ) -> Optional[Result]:
        channel = str(request.payload.get("channel", "")).strip()
        notice_id = str(request.payload.get("notice_id", "")).strip()
        if not channel or not notice_id or not self._store.notice_seen(channel, notice_id):
            return None
        entry_id = None
        for ev in events:
            d = ev.get("data", {})
            if d.get("channel") == channel and d.get("notice_id") == notice_id:
                entry_id = d.get("entry_id")
        data = {"notice_id": notice_id, "channel": channel, "replayed": True}
        if entry_id:
            data["entry_id"] = entry_id
            if entry_id in view.entries:
                data["entry_phase"] = view.entries[entry_id].phase
        return Result(True, view.phase, "通知为重放，只认原通知对应报名，状态未变更", data)

    # ---------- 查询 / 争议还原 ----------

    def _handle_query(self, request: Request) -> Result:
        race_id = str(request.payload.get("race_id", "")).strip()
        if not race_id:
            return Result(False, "rejected", "缺少 race_id", {})
        events = self._store.load_events(race_id)
        view = fold(events)
        if not view.exists:
            return Result(False, "nonexistent", "赛事不存在", {})

        if request.action == "get_race":
            summary = race_summary(view)
            if request.actor != view.admin:
                scopes = view.staff.get(request.actor)
                if not scopes:
                    return Result(False, "forbidden", "无权查看该赛事的报名数据", {})
                if ALL_GROUPS not in scopes:
                    summary["groups"] = {
                        gid: g for gid, g in summary["groups"].items() if gid in scopes
                    }
                    summary["entries"] = {
                        eid: e for eid, e in summary["entries"].items()
                        if e["group_id"] in scopes
                    }
            return Result(True, view.phase, "ok", summary)

        if request.action == "get_waitlist":
            group_id = str(request.payload.get("group_id", "")).strip()
            if group_id not in view.groups:
                return Result(False, view.phase, "组别不存在", {})
            self._require_read_auth(view, request.actor, group_id)
            order = [
                {
                    "entry_id": e.entry_id,
                    "id_no": e.registrant["id_no"],
                    "name": e.registrant["name"],
                    "registered_seq": e.registered_seq,
                }
                for e in self._waitlist(view, group_id)
            ]
            return Result(True, view.phase, "ok", {"group_id": group_id, "waitlist": order})

        if request.action == "get_entry":
            entry_id = str(request.payload.get("entry_id", "")).strip()
            entry = view.entries.get(entry_id)
            if entry is None:
                return Result(False, view.phase, "报名不存在", {})
            self._require_read_auth(view, request.actor, entry.group_id, entry)
            summary = race_summary(view)["entries"][entry_id]
            return Result(True, entry.phase, "ok", summary)

        if request.action == "slot_history":
            slot_id = str(request.payload.get("slot_id", "")).strip()
            if not slot_id:
                return Result(False, view.phase, "缺少 slot_id", {})
            group_id = slot_id.rsplit("-S", 1)[0]
            if group_id not in view.groups:
                return Result(False, view.phase, "名额不存在", {})
            self._require_read_auth(view, request.actor, group_id)
            timeline = slot_history(events, slot_id)
            if not timeline:
                return Result(False, view.phase, "名额尚未发放", {})
            return Result(True, view.phase, "ok", {
                "slot_id": slot_id,
                "group_id": group_id,
                "timeline": timeline,
            })

        if request.action == "entry_history":
            entry_id = str(request.payload.get("entry_id", "")).strip()
            entry = view.entries.get(entry_id)
            if entry is None:
                return Result(False, view.phase, "报名不存在", {})
            self._require_read_auth(view, request.actor, entry.group_id, entry)
            return Result(True, entry.phase, "ok", {
                "entry_id": entry_id,
                "timeline": entry_history(events, entry_id),
            })

        return Result(False, "unknown", f"未知动作 {request.action}", {})

    # ---------- 鉴权 ----------

    @staticmethod
    def _require_read_auth(view, actor: str, group_id: str, entry=None) -> None:
        if actor == view.admin:
            return
        if entry is not None and actor == entry.registrant["id_no"]:
            return
        scopes = view.staff.get(actor, [])
        if ALL_GROUPS in scopes or group_id in scopes:
            return
        raise DomainError(f"无权查看组别 {group_id} 的数据", "forbidden")

    @staticmethod
    def _waitlist(view, group_id: str):
        from .domain import waitlist_order
        return waitlist_order(view, group_id)


def _success_message(action: str) -> str:
    return {
        "create_race": "赛事已创建",
        "add_group": "组别已添加",
        "authorize_staff": "工作人员授权已生效",
        "open_registration": "报名已开放",
        "close_registration": "报名已截止",
        "register": "报名已受理",
        "release_quota": "名额批次已发放，候补已按序递补",
        "run_lottery": "抽签已完成（结果未公布）",
        "publish_lottery": "抽签结果已公布，中签名额已按序占用",
        "payment_notice": "支付已确认",
        "refund_notice": "退款已结清",
        "request_withdrawal": "退赛申请已受理",
        "complete_withdrawal": "退赛已完成，释放名额已交给下一位候补",
    }.get(action, "已受理")
