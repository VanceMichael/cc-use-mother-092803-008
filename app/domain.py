"""赛事报名核心领域：事件折叠投影 + 命令决策。

本模块是纯函数式核心：``fold`` 把事件流还原成当前状态，``decide`` 在当前状态上
判定一条命令是否合法、合法时产出哪些事件。所有写入都由 service 层在一个事务内
追加到事件流，状态只能前进，不能被直接修改。

名额生命周期（也是争议还原的时间线）::

    QuotaIssued 发放 → SlotHeld 占用 → PaymentRecorded 付款
        → SlotFreed 释放(退款完成/退赛完成) → SlotHeld 下一位候补递补 → …

关键不变量：
* 同一证件在整个赛事只有一条报名（含已终止），重复提交一律合并或拒绝；
* 任何时刻“空闲名额”与“候补队列”不能同时非空——名额一释放立即按确定顺序递补；
* 候补顺序：先到先得组按报名先后；抽签组按抽签排名（抽签结果固化在事件中）；
* 退赛申请不释放名额，完成退赛（或退款完成）才释放并立即递补；
* 报名截止、抽签结果公布均单向不可逆，之后的任何迟到通知都不能复活旧报名。
"""
from __future__ import annotations

import random
import secrets
from dataclasses import dataclass, field
from typing import Any, Optional

# ---------------- 事件类型 ----------------

RACE_CREATED = "RaceCreated"
GROUP_ADDED = "GroupAdded"
STAFF_AUTHORIZED = "StaffAuthorized"
REGISTRATION_OPENED = "RegistrationOpened"
REGISTRATION_CLOSED = "RegistrationClosed"
QUOTA_ISSUED = "QuotaIssued"
ENTRY_REGISTERED = "EntryRegistered"
SLOT_HELD = "SlotHeld"
PAYMENT_RECORDED = "PaymentRecorded"
WITHDRAWAL_REQUESTED = "WithdrawalRequested"
SLOT_FREED = "SlotFreed"
ENTRY_TERMINATED = "EntryTerminated"
PAYMENT_REFUNDED = "PaymentRefunded"
LOTTERY_DRAWN = "LotteryDrawn"
LOTTERY_PUBLISHED = "LotteryPublished"

SLOT_TIMELINE_EVENTS = {
    QUOTA_ISSUED,
    SLOT_HELD,
    PAYMENT_RECORDED,
    SLOT_FREED,
    PAYMENT_REFUNDED,
}
ENTRY_TIMELINE_EVENTS = SLOT_TIMELINE_EVENTS | {
    ENTRY_REGISTERED,
    WITHDRAWAL_REQUESTED,
    ENTRY_TERMINATED,
    LOTTERY_DRAWN,
}

MODE_FCFS = "fcfs"            # 分批放号，先到先得 + 候补
MODE_LOTTERY = "lottery"      # 截止后抽签，按抽签排名占用与递补

ALL_GROUPS = "*"


class DomainError(Exception):
    """命令被领域规则拒绝；不会产生任何事件。"""

    def __init__(self, message: str, state: str = "rejected", data: Optional[dict] = None) -> None:
        super().__init__(message)
        self.message = message
        self.state = state
        self.data = data or {}


# ---------------- 投影 ----------------

@dataclass
class GroupView:
    group_id: str
    name: str
    capacity: int
    fee: int
    mode: str
    issued: int = 0
    entry_seq: int = 0
    batches: set[str] = field(default_factory=set)
    lottery_order: list[str] = field(default_factory=list)  # entry_id 抽签排名
    drawn: bool = False


@dataclass
class EntryView:
    entry_id: str
    group_id: str
    registrant: dict[str, str]
    client_token: Optional[str]
    payment_id: str
    registered_seq: int                       # EntryRegistered 在流中的序号
    phase: str = "registered"                 # registered|held|paid|withdrawing|terminated
    slot_id: Optional[str] = None
    paid: bool = False
    refunded: bool = False
    refund_due: bool = False                  # 已退赛占名额释放，但退款尚未结清
    terminate_reason: Optional[str] = None


@dataclass
class RaceView:
    race_id: str = ""
    name: str = ""
    phase: str = "nonexistent"                # setup|open|closed|published
    admin: str = ""
    groups: dict[str, GroupView] = field(default_factory=dict)
    entries: dict[str, EntryView] = field(default_factory=dict)
    payments: dict[str, str] = field(default_factory=dict)        # payment_id -> entry_id
    id_index: dict[tuple[str, str], str] = field(default_factory=dict)
    client_tokens: dict[str, str] = field(default_factory=dict)
    staff: dict[str, list[str]] = field(default_factory=dict)
    slots: dict[str, str] = field(default_factory=dict)          # slot_id -> free|held
    slot_holder: dict[str, str] = field(default_factory=dict)    # slot_id -> entry_id
    last_seq: int = 0                        # 流中最后一个事件的 stream_seq
    exists: bool = False


def fold(events: list[dict[str, Any]]) -> RaceView:
    view = RaceView()
    for event in events:
        apply_event(view, event["event_type"], event.get("data", {}), event.get("stream_seq", 0))
    return view


def apply_event(view: RaceView, event_type: str, d: dict[str, Any], stream_seq: int = 0) -> None:
    view.last_seq = max(view.last_seq, stream_seq)
    if event_type == RACE_CREATED:
        view.exists = True
        view.race_id = d["race_id"]
        view.name = d["name"]
        view.phase = "setup"
        view.admin = d["by"]
    elif event_type == GROUP_ADDED:
        view.groups[d["group_id"]] = GroupView(
            d["group_id"], d["name"], d["capacity"], d["fee"], d["mode"]
        )
    elif event_type == STAFF_AUTHORIZED:
        view.staff[d["staff_id"]] = list(d["group_ids"]) if d.get("group_ids") else [ALL_GROUPS]
    elif event_type == REGISTRATION_OPENED:
        view.phase = "open"
    elif event_type == REGISTRATION_CLOSED:
        view.phase = "closed"
    elif event_type == LOTTERY_PUBLISHED:
        view.phase = "published"
    elif event_type == QUOTA_ISSUED:
        view.slots[d["slot_id"]] = "free"
        group = view.groups[d["group_id"]]
        group.issued += 1
        group.batches.add(d.get("batch_id", ""))
    elif event_type == ENTRY_REGISTERED:
        group = view.groups[d["group_id"]]
        group.entry_seq += 1
        entry = EntryView(
            entry_id=d["entry_id"],
            group_id=d["group_id"],
            registrant=dict(d["registrant"]),
            client_token=d.get("client_token"),
            payment_id=d["payment_id"],
            registered_seq=stream_seq,
        )
        view.entries[d["entry_id"]] = entry
        view.payments[d["payment_id"]] = d["entry_id"]
        view.id_index[(d["registrant"]["id_type"], d["registrant"]["id_no"])] = d["entry_id"]
        if d.get("client_token"):
            view.client_tokens[d["client_token"]] = d["entry_id"]
    elif event_type == SLOT_HELD:
        view.slots[d["slot_id"]] = "held"
        view.slot_holder[d["slot_id"]] = d["entry_id"]
        entry = view.entries[d["entry_id"]]
        entry.phase = "held"
        entry.slot_id = d["slot_id"]
    elif event_type == PAYMENT_RECORDED:
        entry = view.entries[d["entry_id"]]
        entry.paid = True
        entry.phase = "paid"
    elif event_type == WITHDRAWAL_REQUESTED:
        view.entries[d["entry_id"]].phase = "withdrawing"
    elif event_type == SLOT_FREED:
        view.slots[d["slot_id"]] = "free"
        view.slot_holder.pop(d["slot_id"], None)
        entry = view.entries[d["entry_id"]]
        entry.slot_id = None
    elif event_type == ENTRY_TERMINATED:
        entry = view.entries[d["entry_id"]]
        entry.phase = "terminated"
        entry.terminate_reason = d["reason"]
        entry.refund_due = bool(d.get("refund_due"))
        entry.slot_id = None
    elif event_type == PAYMENT_REFUNDED:
        entry = view.entries[d["entry_id"]]
        entry.refunded = True
        entry.refund_due = False
    elif event_type == LOTTERY_DRAWN:
        group = view.groups[d["group_id"]]
        group.lottery_order = list(d["ordering"])
        group.drawn = True


# ---------------- 决策 ----------------

@dataclass
class Command:
    actor: str
    action: str
    payload: dict[str, Any]


def decide(view: RaceView, cmd: Command, now: str, seed: Optional[int] = None) -> list[dict[str, Any]]:
    """返回待追加事件列表（每项 {event_type, data, notices?}）。

    拒绝时抛 DomainError。data 中统一带操作人 by。
    """
    p = cmd.payload
    actor = cmd.actor
    events: list[dict[str, Any]] = []

    def emit(event_type: str, data: dict[str, Any], notices: Optional[list[tuple[str, str]]] = None) -> None:
        data = dict(data)
        data.setdefault("by", actor)
        item: dict[str, Any] = {"event_type": event_type, "data": data}
        if notices:
            item["notices"] = notices
        events.append(item)
        view.last_seq += 1
        apply_event(view, event_type, data, view.last_seq)

    def require_race() -> None:
        if not view.exists:
            raise DomainError("赛事不存在", "nonexistent")

    def is_admin() -> bool:
        return actor == view.admin

    def require_admin() -> None:
        if not is_admin():
            raise DomainError("该操作仅赛事管理员可执行", view.phase)

    def require_group_auth(group_id: str) -> GroupView:
        require_race()
        group = view.groups.get(group_id)
        if group is None:
            raise DomainError(f"组别 {group_id} 不存在", view.phase)
        if not is_admin():
            scopes = view.staff.get(actor, [])
            if ALL_GROUPS not in scopes and group_id not in scopes:
                raise DomainError(f"工作人员 {actor} 未获组别 {group_id} 的授权", "forbidden")
        return group

    # ---------- 赛事与组别管理 ----------

    if cmd.action == "create_race":
        if view.exists:
            raise DomainError("赛事已存在，不能重复创建", view.phase)
        name = _text(p, "name")
        race_id = _text(p, "race_id")
        emit(RACE_CREATED, {"race_id": race_id, "name": name})
        return events

    if cmd.action == "add_group":
        require_race()
        require_admin()
        group_id = _text(p, "group_id")
        if group_id in view.groups:
            raise DomainError(f"组别 {group_id} 已存在", view.phase)
        capacity = _positive_int(p, "capacity")
        fee = _nonnegative_int(p, "fee")
        mode = p.get("mode", MODE_FCFS)
        if mode not in (MODE_FCFS, MODE_LOTTERY):
            raise DomainError("组别分配方式只能是 fcfs 或 lottery", view.phase)
        emit(GROUP_ADDED, {
            "group_id": group_id,
            "name": _text(p, "name"),
            "capacity": capacity,
            "fee": fee,
            "mode": mode,
        })
        return events

    if cmd.action == "authorize_staff":
        require_race()
        require_admin()
        staff_id = _text(p, "staff_id")
        group_ids = p.get("group_ids", [ALL_GROUPS])
        if group_ids is not None and group_ids != [ALL_GROUPS]:
            for gid in group_ids:
                if gid not in view.groups:
                    raise DomainError(f"授权目标组别 {gid} 不存在", view.phase)
        else:
            group_ids = [ALL_GROUPS]
        emit(STAFF_AUTHORIZED, {"staff_id": staff_id, "group_ids": list(group_ids)})
        return events

    if cmd.action == "open_registration":
        require_race()
        require_admin()
        if view.phase != "setup":
            raise DomainError(f"当前阶段 {view.phase} 不能开放报名（状态不可倒退）", view.phase)
        if not view.groups:
            raise DomainError("尚未设置任何组别", view.phase)
        emit(REGISTRATION_OPENED, {"at": now})
        return events

    if cmd.action == "close_registration":
        require_race()
        require_admin()
        if view.phase != "open":
            raise DomainError("只有报名进行中才能截止；截止后不能重新开放", view.phase)
        emit(REGISTRATION_CLOSED, {"at": now})
        return events

    # ---------- 报名 ----------

    if cmd.action == "register":
        require_race()
        if view.phase != "open":
            raise DomainError(f"当前阶段 {view.phase} 不接受报名", view.phase)
        group_id = _text(p, "group_id")
        group = view.groups.get(group_id)
        if group is None:
            raise DomainError(f"组别 {group_id} 不存在", view.phase)
        r = p.get("registrant")
        if not isinstance(r, dict):
            raise DomainError("缺少报名人信息", view.phase)
        id_type = _text(r, "id_type", "registrant.id_type")
        id_no = _text(r, "id_no", "registrant.id_no")
        name = _text(r, "name", "registrant.name")
        if actor != id_no:
            raise DomainError("报名操作人必须与报名证件一致", view.phase)
        client_token = str(p.get("client_token", "")).strip() or None
        if client_token and client_token in view.client_tokens:
            # 同一提交令牌的连续点击：合并到原报名，不再产生任何占用。
            raise _RepeatedSubmission(view.client_tokens[client_token])
        fingerprint = (id_type, id_no)
        if fingerprint in view.id_index:
            # 同证件再次提交（无提交令牌或换了令牌）：拒绝，绝不产生第二个名额占用。
            raise DomainError(
                "该证件在此赛事已有报名记录，重复提交不能再占名额",
                "duplicate_registration",
                {"entry_id": view.id_index[fingerprint]},
            )
        next_no = group.entry_seq + 1
        entry_id = f"{group.group_id}-E{next_no:04d}"
        payment_id = f"PAY-{entry_id}"
        emit(ENTRY_REGISTERED, {
            "entry_id": entry_id,
            "group_id": group.group_id,
            "registrant": {"id_type": id_type, "id_no": id_no, "name": name},
            "client_token": client_token,
            "payment_id": payment_id,
        })
        # 先到先得组：若恰好有已放未占名额（说明候补为空），新报名直接占用。
        if group.mode == MODE_FCFS:
            _fill_waitlist(view, events, group.group_id, emit, mode="direct")
        return events

    # ---------- 分批放号 ----------

    if cmd.action == "release_quota":
        require_race()
        group_id = _text(p, "group_id")
        group = require_group_auth(group_id)
        if view.phase != "open":
            raise DomainError("报名截止后不能再放新号", view.phase)
        if group.mode != MODE_FCFS:
            raise DomainError("抽签组别不手工放号，名额以容量为准", view.phase)
        count = _positive_int(p, "count")
        batch_id = _text(p, "batch_id")
        if batch_id in group.batches:
            raise DomainError(f"放号批次 {batch_id} 已处理，禁止重复放号", view.phase)
        if group.issued + count > group.capacity:
            raise DomainError(
                f"放号后总数 {group.issued + count} 超过组别容量 {group.capacity}",
                view.phase,
            )
        group.batches.add(batch_id)
        for _ in range(count):
            slot_id = f"{group_id}-S{group.issued + 1:04d}"
            emit(QUOTA_ISSUED, {
                "slot_id": slot_id,
                "group_id": group_id,
                "batch_id": batch_id,
            })
        # 全部新号按 slot_id 顺序一次性匹配候补队首，顺序确定。
        _fill_waitlist(view, events, group_id, emit, mode="waitlist")
        return events

    # ---------- 抽签 ----------

    if cmd.action == "run_lottery":
        require_race()
        require_admin()
        if view.phase != "closed":
            raise DomainError("抽签只能在报名截止后、结果公布前进行", view.phase)
        lottery_groups = [g for g in view.groups.values() if g.mode == MODE_LOTTERY]
        if not lottery_groups:
            raise DomainError("本赛事没有抽签组别", view.phase)
        draw_seed = seed if seed is not None else int(p.get("seed") or secrets.randbits(63))
        for group in lottery_groups:
            if group.drawn:
                raise DomainError(f"组别 {group.group_id} 抽签结果已生成，不能重抽", view.phase)
            candidates = [
                e.entry_id
                for e in view.entries.values()
                if e.group_id == group.group_id and e.phase == "registered"
            ]
            ordering = sorted(candidates, key=lambda eid: view.entries[eid].registered_seq)
            random.Random(f"{draw_seed}:{group.group_id}").shuffle(ordering)
            emit(LOTTERY_DRAWN, {
                "group_id": group.group_id,
                "ordering": ordering,
                "seed": str(draw_seed),
            })
        return events

    if cmd.action == "publish_lottery":
        require_race()
        require_admin()
        if view.phase != "closed":
            raise DomainError("只能在报名截止后公布抽签结果", view.phase)
        lottery_groups = [g for g in view.groups.values() if g.mode == MODE_LOTTERY]
        if not lottery_groups or any(not g.drawn for g in lottery_groups):
            raise DomainError("尚有抽签组别未完成抽签", view.phase)
        emit(LOTTERY_PUBLISHED, {
            "at": now,
            "groups": {g.group_id: min(g.capacity, len(g.lottery_order)) for g in lottery_groups},
        })
        for group in lottery_groups:
            winners = group.lottery_order[: group.capacity]
            for idx, entry_id in enumerate(winners, start=1):
                slot_id = f"{group.group_id}-S{idx:04d}"
                emit(QUOTA_ISSUED, {
                    "slot_id": slot_id,
                    "group_id": group.group_id,
                    "batch_id": "lottery",
                })
                emit(SLOT_HELD, {
                    "slot_id": slot_id,
                    "entry_id": entry_id,
                    "group_id": group.group_id,
                    "mode": "lottery",
                })
        return events

    # ---------- 支付 / 退款通知 ----------

    if cmd.action == "payment_notice":
        require_race()
        channel, notice_id = _channel_notice(cmd)
        payment_id = _text(p, "payment_id")
        entry_id = view.payments.get(payment_id)
        if entry_id is None:
            raise DomainError(f"支付单 {payment_id} 不存在，通知不能创建或占用名额", view.phase)
        entry = view.entries[entry_id]
        amount = _nonnegative_int(p, "amount")
        if amount != view.groups[entry.group_id].fee:
            raise DomainError(
                f"支付金额 {amount} 与应付 {view.groups[entry.group_id].fee} 不符",
                entry.phase,
            )
        if entry.phase == "paid":
            raise DomainError("该报名已付款，重复支付不再受理", "paid", {"entry_id": entry_id})
        if entry.phase == "terminated":
            # 退款释放名额之后的迟到付款：坚决拒绝，不能让已交出的席位被重新占用。
            raise DomainError(
                "报名已终止且名额已释放，迟到付款不得重新占用名额",
                "terminated",
                {"entry_id": entry_id},
            )
        if entry.phase != "held":
            raise DomainError(
                "只有已占用名额、待付款的报名可以确认支付（候补或退赛中均不可）",
                entry.phase,
                {"entry_id": entry_id},
            )
        emit(PAYMENT_RECORDED, {
            "channel": channel,
            "notice_id": notice_id,
            "payment_id": payment_id,
            "entry_id": entry_id,
            "group_id": entry.group_id,
            "slot_id": entry.slot_id,
            "amount": amount,
        }, notices=[(channel, notice_id)])
        return events

    if cmd.action == "refund_notice":
        require_race()
        channel, notice_id = _channel_notice(cmd)
        payment_id = _text(p, "payment_id")
        entry_id = view.payments.get(payment_id)
        if entry_id is None:
            raise DomainError(f"支付单 {payment_id} 不存在", view.phase)
        entry = view.entries[entry_id]
        amount = _nonnegative_int(p, "amount")
        if not entry.paid:
            raise DomainError("该报名尚未付款，无款可退", entry.phase, {"entry_id": entry_id})
        if entry.refunded:
            raise DomainError("该笔支付已退款，退款通知重放不得再次生效", "refunded",
                              {"entry_id": entry_id})
        if amount != view.groups[entry.group_id].fee:
            raise DomainError(
                f"退款金额 {amount} 与实付 {view.groups[entry.group_id].fee} 不符",
                entry.phase,
            )
        if entry.phase == "terminated":
            # 先完成退赛、释放名额，退款通知后到：只结清款项，名额早已递补，不再变动。
            if not entry.refund_due:
                raise DomainError("报名已终止且无待结退款", "terminated", {"entry_id": entry_id})
            emit(PAYMENT_REFUNDED, {
                "channel": channel,
                "notice_id": notice_id,
                "payment_id": payment_id,
                "entry_id": entry_id,
                "group_id": entry.group_id,
                "slot_id": None,
                "amount": amount,
            }, notices=[(channel, notice_id)])
            return events
        # held 不可能（held 表示未付款）；这里 phase 为 paid 或 withdrawing。
        if entry.phase not in ("paid", "withdrawing"):
            raise DomainError("当前报名状态不能退款", entry.phase, {"entry_id": entry_id})
        slot_id = entry.slot_id
        emit(PAYMENT_REFUNDED, {
            "channel": channel,
            "notice_id": notice_id,
            "payment_id": payment_id,
            "entry_id": entry_id,
            "group_id": entry.group_id,
            "slot_id": slot_id,
            "amount": amount,
        }, notices=[(channel, notice_id)])
        if slot_id is not None:
            # 退款完成 → 名额才可释放并立即交给下一位候补。
            emit(SLOT_FREED, {
                "slot_id": slot_id,
                "entry_id": entry_id,
                "group_id": entry.group_id,
                "reason": "refund",
            })
            emit(ENTRY_TERMINATED, {
                "entry_id": entry_id,
                "group_id": entry.group_id,
                "slot_id": None,
                "reason": "refund",
                "refund_due": False,
            })
            _fill_waitlist(view, events, entry.group_id, emit, mode="waitlist")
        return events

    # ---------- 退赛 ----------

    if cmd.action == "request_withdrawal":
        require_race()
        entry = _entry_by_id(view, _text(p, "entry_id"))
        group = view.groups[entry.group_id]
        if not (actor == entry.registrant["id_no"] or is_admin()
                or ALL_GROUPS in view.staff.get(actor, [])
                or entry.group_id in view.staff.get(actor, [])):
            raise DomainError("无权替该报名申请退赛", "forbidden")
        if entry.phase == "registered":
            # 候补者本就没有占用名额：直接终止，从候补队列移除，不影响任何席位。
            emit(ENTRY_TERMINATED, {
                "entry_id": entry.entry_id,
                "group_id": entry.group_id,
                "slot_id": None,
                "reason": "withdraw",
                "refund_due": False,
            })
            return events
        if entry.phase in ("held", "paid"):
            # 仅登记退赛意愿：名额继续占用，候补不前进，等待退款或工作人员完成退赛。
            emit(WITHDRAWAL_REQUESTED, {"entry_id": entry.entry_id, "reason": p.get("reason", "voluntary")})
            return events
        raise DomainError("当前报名状态不能申请退赛", entry.phase, {"entry_id": entry.entry_id})

    if cmd.action == "complete_withdrawal":
        require_race()
        entry = _entry_by_id(view, _text(p, "entry_id"))
        require_group_auth(entry.group_id)
        if entry.phase != "withdrawing":
            raise DomainError("只有退赛处理中的报名可以完成退赛", entry.phase,
                              {"entry_id": entry.entry_id})
        refund_due = entry.paid and not entry.refunded
        if entry.slot_id is not None:
            # 退赛完成：此刻名额才释放，立即按确定顺序递补。
            emit(SLOT_FREED, {
                "slot_id": entry.slot_id,
                "entry_id": entry.entry_id,
                "group_id": entry.group_id,
                "reason": "withdraw",
            })
        emit(ENTRY_TERMINATED, {
            "entry_id": entry.entry_id,
            "group_id": entry.group_id,
            "slot_id": None,
            "reason": "withdraw",
            "refund_due": refund_due,
        })
        _fill_waitlist(view, events, entry.group_id, emit, mode="waitlist")
        return events

    raise DomainError(f"未知动作 {cmd.action}", "unknown")


# ---------------- 候补递补（确定性） ----------------

def waitlist_order(view: RaceView, group_id: str) -> list[EntryView]:
    """返回该组别当前候补报名的确定顺序。

    抽签组别且已出签：按抽签排名；其余（先到先得组、抽签前）：按报名事件先后。
    已终止报名不参与。
    """
    group = view.groups[group_id]
    waiting = [
        e for e in view.entries.values()
        if e.group_id == group_id and e.phase == "registered" and e.slot_id is None
    ]
    if group.drawn:
        rank = {eid: idx for idx, eid in enumerate(group.lottery_order)}
        waiting = [e for e in waiting if e.entry_id in rank]
        waiting.sort(key=lambda e: rank[e.entry_id])
    else:
        waiting.sort(key=lambda e: e.registered_seq)
    return waiting


def free_slots(view: RaceView, group_id: str) -> list[str]:
    return sorted(
        slot_id for slot_id, state in view.slots.items()
        if state == "free" and _slot_group(slot_id) == group_id
    )


def _fill_waitlist(view: RaceView, events: list[dict], group_id: str, emit, *, mode: str) -> None:
    """把空闲名额按确定顺序逐个交给候补队首。

    维持核心不变量：处理结束后，空闲名额与候补队列不会同时非空。
    """
    while True:
        slots = free_slots(view, group_id)
        candidates = waitlist_order(view, group_id)
        if not slots or not candidates:
            return
        slot_id = slots[0]
        entry = candidates[0]
        emit(SLOT_HELD, {
            "slot_id": slot_id,
            "entry_id": entry.entry_id,
            "group_id": group_id,
            "mode": mode,
            "after_entry_id": None,
        })


def _slot_group(slot_id: str) -> str:
    return slot_id.rsplit("-S", 1)[0]


# ---------------- 查询投影 ----------------

def race_summary(view: RaceView) -> dict[str, Any]:
    groups = {}
    for gid, g in view.groups.items():
        waiting = [e.entry_id for e in waitlist_order(view, gid)]
        groups[gid] = {
            "name": g.name,
            "mode": g.mode,
            "capacity": g.capacity,
            "issued": g.issued,
            "held": sum(1 for s, st in view.slots.items() if st == "held" and _slot_group(s) == gid),
            "paid": sum(1 for e in view.entries.values()
                        if e.group_id == gid and e.phase == "paid"),
            "terminated": sum(
                1 for e in view.entries.values()
                if e.group_id == gid and e.phase == "terminated"
            ),
            "waitlist": waiting,
            "lottery_order": list(g.lottery_order) if g.drawn else None,
        }
    entries = {
        eid: {
            "entry_id": e.entry_id,
            "group_id": e.group_id,
            "registrant": e.registrant,
            "phase": e.phase,
            "slot_id": e.slot_id,
            "payment_id": e.payment_id,
            "paid": e.paid,
            "refunded": e.refunded,
            "refund_due": e.refund_due,
            "terminate_reason": e.terminate_reason,
        }
        for eid, e in view.entries.items()
    }
    return {
        "race_id": view.race_id,
        "name": view.name,
        "phase": view.phase,
        "admin": view.admin,
        "staff": view.staff,
        "groups": groups,
        "entries": entries,
    }


def slot_history(events: list[dict[str, Any]], slot_id: str) -> list[dict[str, Any]]:
    """还原指定名额从发放、占用、付款到释放/退款、再递补的全部事件。"""
    timeline = []
    for ev in events:
        d = ev.get("data", {})
        if ev["event_type"] in SLOT_TIMELINE_EVENTS and d.get("slot_id") == slot_id:
            timeline.append(_public_event(ev))
    return timeline


def entry_history(events: list[dict[str, Any]], entry_id: str) -> list[dict[str, Any]]:
    timeline = []
    for ev in events:
        d = ev.get("data", {})
        if ev["event_type"] in ENTRY_TIMELINE_EVENTS and (
            d.get("entry_id") == entry_id
            or (ev["event_type"] == LOTTERY_DRAWN and entry_id in d.get("ordering", []))
        ):
            timeline.append(_public_event(ev))
    return timeline


def _public_event(ev: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": ev["event_id"],
        "stream_seq": ev["stream_seq"],
        "event_type": ev["event_type"],
        "actor": ev.get("actor"),
        "occurred_at": ev.get("occurred_at"),
        "data": ev.get("data", {}),
    }


# ---------------- 辅助 ----------------

class _RepeatedSubmission(DomainError):
    def __init__(self, entry_id: str) -> None:
        super().__init__("同提交令牌的重复报名已合并", "already_registered", {"entry_id": entry_id})


def _text(data: dict[str, Any], key: str, label: Optional[str] = None) -> str:
    value = str(data.get(key, "")).strip()
    if not value:
        raise DomainError(f"缺少必填字段 {label or key}")
    return value


def _positive_int(data: dict[str, Any], key: str) -> int:
    raw = data.get(key)
    if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
        raise DomainError(f"字段 {key} 必须是正整数")
    return raw


def _nonnegative_int(data: dict[str, Any], key: str) -> int:
    raw = data.get(key)
    if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
        raise DomainError(f"字段 {key} 必须是非负整数（单位：分）")
    return raw


def _channel_notice(cmd: Command) -> tuple[str, str]:
    p = cmd.payload
    channel = _text(p, "channel")
    notice_id = _text(p, "notice_id")
    if cmd.actor != f"payment:{channel}":
        raise DomainError(f"支付通道通知必须由 payment:{channel} 身份提交", "forbidden")
    return channel, notice_id


def _entry_by_id(view: RaceView, entry_id: str) -> EntryView:
    entry = view.entries.get(entry_id)
    if entry is None:
        raise DomainError(f"报名 {entry_id} 不存在", view.phase)
    return entry
