"""赛事报名领域服务。

设计要点：
* 每个写命令都在一个 BEGIN IMMEDIATE 事务内完成，事件日志先于状态落库；
* request_id 幂等：重放返回首次结果，绝不二次执行；
* 名额是具身份实体（category_id + seq），放号、占用、付款、释放、递补全部留痕；
* 退赛只在“退款完成”通知到达后才释放名额并触发一次且仅一次候补递补；
* 报名截止 / 抽签公布后只允许向前流转，终态记录不可变更；
* 工作人员的组别操作必须命中 grants 授权。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from typing import Any

from . import actions as A
from .contracts import Conflict, DomainError, Forbidden, NotFound, Request, Result
from .store import Store, utcnow

ACTIVE_STATES = ("held", "paid", "refund_pending")
PROMOTABLE_CATEGORY_STATUSES = (A.CAT_OPEN, A.CAT_CLOSED)
QUERY_ACTIONS = {A.SLOT_TIMELINE, A.CATEGORY_LIST, A.REGISTRANT_LOOKUP}


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _require(payload: dict[str, Any], key: str) -> Any:
    value = payload.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise DomainError(f"缺少参数 {key}", "bad_request")
    return value


class MarathonEntryService:
    def __init__(self, store: Store | str = ":memory:") -> None:
        self.store = store if isinstance(store, Store) else Store(store)
        self._lock = threading.RLock()  # 单连接内串行，跨进程靠 BEGIN IMMEDIATE

    def close(self) -> None:
        self.store.conn.close()

    # ================================================================== 入口
    def handle(self, request: Request) -> Result:
        if not request.actor or not request.action:
            raise ValueError("请求缺少身份或动作")
        if not isinstance(request.payload, dict):
            raise TypeError("请求数据必须是对象")
        is_query = request.action in QUERY_ACTIONS
        if not is_query:
            if not request.request_id:
                raise ValueError("写请求缺少幂等键 request_id")
            cached = self._load_idempotent(request.request_id)
            if cached is not None:
                return cached
        try:
            with self._lock, self.store.tx() as tx:
                if not is_query:
                    # 拿到写锁后复查：跨进程并发时首个请求可能刚刚提交
                    cached = self._load_idempotent(request.request_id)
                    if cached is not None:
                        return cached
                result = self._dispatch(tx, request)
                if not is_query:
                    self._save_idempotent(tx, request, result)
                return result
        except DomainError as exc:
            result = Result.reject(exc.message, exc.code)
            if not is_query and request.request_id:
                # 业务拒绝也记幂等：重试同样的坏请求不会反复制造副作用
                with self._lock, self.store.tx() as tx:
                    self._save_idempotent(tx, request, result, ignore_duplicate=True)
            return result

    # ----------------------------------------------------------- 幂等等待
    def _load_idempotent(self, request_id: str) -> Result | None:
        row = self.store.conn.execute(
            "SELECT accepted, state, message, data FROM idempotency WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            return None
        return Result(
            bool(row["accepted"]),
            row["state"],
            row["message"],
            json.loads(row["data"]),
        )

    def _save_idempotent(
        self, tx: sqlite3.Connection, request: Request, result: Result, *, ignore_duplicate: bool = False
    ) -> None:
        sql = (
            "INSERT OR IGNORE INTO idempotency "
            "(request_id, action, accepted, state, message, data, created_at) VALUES (?,?,?,?,?,?,?)"
            if ignore_duplicate
            else "INSERT INTO idempotency "
            "(request_id, action, accepted, state, message, data, created_at) VALUES (?,?,?,?,?,?,?)"
        )
        tx.execute(
            sql,
            (
                request.request_id,
                request.action,
                int(result.accepted),
                result.state,
                result.message,
                json.dumps(result.data, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )

    def _dispatch(self, tx: sqlite3.Connection, req: Request) -> Result:
        table = {
            A.EVENT_CREATE: self._event_create,
            A.EVENT_CLOSE_REGISTRATION: self._event_close,
            A.EVENT_PUBLISH_LOTTERY: self._event_publish_lottery,
            A.CATEGORY_CREATE: self._category_create,
            A.CATEGORY_OPEN_BATCH: self._category_open_batch,
            A.CATEGORY_GRANT: self._category_grant,
            A.CATEGORY_REVOKE: self._category_revoke,
            A.REGISTRANT_REGISTER: self._register,
            A.REGISTRANT_WITHDRAW: self._withdraw,
            A.STAFF_CANCEL: self._staff_cancel,
            A.PAYMENT_NOTIFY: self._payment_notify,
            A.REFUND_NOTIFY: self._refund_notify,
            A.SLOT_TIMELINE: self._slot_timeline,
            A.CATEGORY_LIST: self._category_list,
            A.REGISTRANT_LOOKUP: self._registrant_lookup,
        }
        handler = table.get(req.action)
        if handler is None:
            raise DomainError(f"未知动作 {req.action}", "bad_request")
        return handler(tx, req)

    # ============================================================== 加载器
    def _get_race(self, tx: sqlite3.Connection, race_id: str) -> sqlite3.Row:
        row = tx.execute("SELECT * FROM races WHERE id = ?", (race_id,)).fetchone()
        if row is None:
            raise NotFound(f"赛事不存在: {race_id}")
        return row

    def _get_category(self, tx: sqlite3.Connection, category_id: str) -> sqlite3.Row:
        row = tx.execute("SELECT * FROM categories WHERE id = ?", (category_id,)).fetchone()
        if row is None:
            raise NotFound(f"组别不存在: {category_id}")
        return row

    def _get_registration(self, tx: sqlite3.Connection, reg_id: str) -> sqlite3.Row:
        row = tx.execute("SELECT * FROM registrations WHERE id = ?", (reg_id,)).fetchone()
        if row is None:
            raise NotFound(f"报名记录不存在: {reg_id}")
        return row

    def _is_race_admin(self, tx: sqlite3.Connection, actor: str, race_id: str) -> bool:
        row = tx.execute("SELECT created_by FROM races WHERE id = ?", (race_id,)).fetchone()
        return row is not None and row["created_by"] == actor

    def _require_race_admin(self, tx: sqlite3.Connection, actor: str, race_id: str) -> None:
        if not self._is_race_admin(tx, actor, race_id):
            raise Forbidden("仅赛事创建者可以执行该操作")

    def _require_category_grant(self, tx: sqlite3.Connection, actor: str, category_id: str) -> None:
        cat = self._get_category(tx, category_id)
        if self._is_race_admin(tx, actor, cat["race_id"]):
            return
        row = tx.execute(
            "SELECT 1 FROM grants WHERE staff_id = ? AND category_id = ?",
            (actor, category_id),
        ).fetchone()
        if row is None:
            raise Forbidden(f"工作人员 {actor} 未获该组别授权")

    # ============================================================== 赛事
    def _event_create(self, tx: sqlite3.Connection, req: Request) -> Result:
        p = req.payload
        race_id = str(p.get("race_id") or _new_id("race")).strip()
        name = str(_require(p, "name")).strip()
        if tx.execute("SELECT 1 FROM races WHERE id = ?", (race_id,)).fetchone():
            raise Conflict(f"赛事已存在: {race_id}")
        tx.execute(
            "INSERT INTO races (id, name, status, created_by, created_at) VALUES (?,?,?,?,?)",
            (race_id, name, "open", req.actor, utcnow()),
        )
        self.store.append_event(
            tx, "RaceCreated", actor=req.actor, race_id=race_id, payload={"name": name}
        )
        return Result.ok("赛事已创建", "race_open", race_id=race_id, name=name)

    def _event_close(self, tx: sqlite3.Connection, req: Request) -> Result:
        race_id = str(_require(req.payload, "race_id"))
        race = self._get_race(tx, race_id)
        self._require_race_admin(tx, req.actor, race_id)
        if race["status"] == "closed":
            raise Conflict("报名已截止，状态不能倒退")
        if race["status"] == "lottery_published":
            raise Conflict("抽签结果已公布，报名不可重新开启")
        tx.execute("UPDATE races SET status = 'closed' WHERE id = ?", (race_id,))
        tx.execute(
            "UPDATE categories SET status = ? WHERE race_id = ? AND status = ?",
            (A.CAT_CLOSED, race_id, A.CAT_OPEN),
        )
        self.store.append_event(
            tx, "RegistrationClosed", actor=req.actor, race_id=race_id
        )
        return Result.ok("报名已截止", "closed", race_id=race_id)

    def _event_publish_lottery(self, tx: sqlite3.Connection, req: Request) -> Result:
        race_id = str(_require(req.payload, "race_id"))
        race = self._get_race(tx, race_id)
        self._require_race_admin(tx, req.actor, race_id)
        if race["status"] == "lottery_published":
            raise Conflict("抽签结果已公布，不可重复公布或回退")
        tx.execute("UPDATE races SET status = 'lottery_published' WHERE id = ?", (race_id,))
        tx.execute(
            "UPDATE categories SET status = ? WHERE race_id = ?",
            (A.CAT_LOTTERY, race_id),
        )
        # 所有候补者落选举——终态，不可回退
        waiting = tx.execute(
            "SELECT * FROM registrations WHERE race_id = ? AND status = 'waiting' ORDER BY waitlist_seq",
            (race_id,),
        ).fetchall()
        for reg in waiting:
            tx.execute(
                "UPDATE registrations SET status = 'lot_out', version = version + 1, updated_at = ? WHERE id = ?",
                (utcnow(), reg["id"]),
            )
            self.store.append_event(
                tx,
                "LotteryResult",
                actor=req.actor,
                race_id=race_id,
                category_id=reg["category_id"],
                registration_id=reg["id"],
                payload={"result": "out"},
            )
        self.store.append_event(
            tx, "LotteryPublished", actor=req.actor, race_id=race_id,
            payload={"lot_out": len(waiting)},
        )
        return Result.ok("抽签结果已公布", A.CAT_LOTTERY,
                         race_id=race_id, lot_out=len(waiting))

    # ============================================================== 组别
    def _category_create(self, tx: sqlite3.Connection, req: Request) -> Result:
        p = req.payload
        race_id = str(_require(p, "race_id"))
        name = str(_require(p, "name")).strip()
        fee_fen = int(p.get("fee_fen", 0))
        quota = int(p.get("quota", 0))
        if fee_fen < 0 or quota < 0:
            raise DomainError("费用与容量必须非负", "bad_request")
        race = self._get_race(tx, race_id)
        self._require_race_admin(tx, req.actor, race_id)
        if race["status"] != "open":
            raise Conflict("赛事报名已截止，不能新增组别")
        category_id = str(p.get("category_id") or _new_id("cat")).strip()
        try:
            tx.execute(
                """INSERT INTO categories
                   (id, race_id, name, fee_fen, quota, status, created_by, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (category_id, race_id, name, fee_fen, quota, A.CAT_OPEN, req.actor, utcnow()),
            )
        except sqlite3.IntegrityError:
            raise Conflict(f"组别名称重复: {name}")
        self.store.append_event(
            tx, "CategoryCreated", actor=req.actor, race_id=race_id,
            category_id=category_id, payload={"name": name, "fee_fen": fee_fen, "quota": quota},
        )
        return Result.ok("组别已创建", A.CAT_OPEN, category_id=category_id, name=name)

    def _category_open_batch(self, tx: sqlite3.Connection, req: Request) -> Result:
        """分批放号：在容量上限内新增一批具 seq 的 free 名额。"""
        p = req.payload
        category_id = str(_require(p, "category_id"))
        count = int(_require(p, "count"))
        cat = self._get_category(tx, category_id)
        self._require_category_grant(tx, req.actor, category_id)
        if cat["status"] != A.CAT_OPEN:
            raise Conflict("报名已截止，不能再放号")
        if count <= 0:
            raise DomainError("放号数量必须为正", "bad_request")
        if cat["quota"] and cat["released"] + count > cat["quota"]:
            raise Conflict(
                f"超出组别容量：已放 {cat['released']}，本批 {count}，上限 {cat['quota']}"
            )
        first_seq = cat["released"] + 1
        for i in range(count):
            seq = first_seq + i
            slot_id = _new_id("slot")
            tx.execute(
                "INSERT INTO slots (id, category_id, seq, status) VALUES (?,?,?,?)",
                (slot_id, category_id, seq, A.SLOT_FREE),
            )
            self.store.append_event(
                tx,
                "SlotIssued",
                actor=req.actor,
                race_id=cat["race_id"],
                category_id=category_id,
                slot_id=slot_id,
                payload={"seq": seq, "batch": req.request_id},
            )
        tx.execute(
            "UPDATE categories SET released = released + ? WHERE id = ?",
            (count, category_id),
        )
        return Result.ok(
            f"已放出 {count} 个名额", "batch_opened",
            category_id=category_id, released=cat["released"] + count,
            first_seq=first_seq, last_seq=first_seq + count - 1,
        )

    def _category_grant(self, tx: sqlite3.Connection, req: Request) -> Result:
        p = req.payload
        category_id = str(_require(p, "category_id"))
        staff_id = str(_require(p, "staff_id"))
        cat = self._get_category(tx, category_id)
        self._require_race_admin(tx, req.actor, cat["race_id"])
        tx.execute(
            "INSERT OR IGNORE INTO grants (staff_id, category_id, granted_by, created_at) VALUES (?,?,?,?)",
            (staff_id, category_id, req.actor, utcnow()),
        )
        self.store.append_event(
            tx, "StaffGranted", actor=req.actor, race_id=cat["race_id"],
            category_id=category_id, payload={"staff_id": staff_id},
        )
        return Result.ok("授权已生效", "granted", category_id=category_id, staff_id=staff_id)

    def _category_revoke(self, tx: sqlite3.Connection, req: Request) -> Result:
        p = req.payload
        category_id = str(_require(p, "category_id"))
        staff_id = str(_require(p, "staff_id"))
        cat = self._get_category(tx, category_id)
        self._require_race_admin(tx, req.actor, cat["race_id"])
        tx.execute(
            "DELETE FROM grants WHERE staff_id = ? AND category_id = ?",
            (staff_id, category_id),
        )
        self.store.append_event(
            tx, "StaffRevoked", actor=req.actor, race_id=cat["race_id"],
            category_id=category_id, payload={"staff_id": staff_id},
        )
        return Result.ok("授权已撤销", "revoked", category_id=category_id, staff_id=staff_id)

    # ============================================================== 报名
    def _register(self, tx: sqlite3.Connection, req: Request) -> Result:
        p = req.payload
        category_id = str(_require(p, "category_id"))
        id_type = str(_require(p, "id_type")).strip()
        id_no = str(_require(p, "id_no")).strip()
        person_name = str(_require(p, "person_name")).strip()
        cat = self._get_category(tx, category_id)
        if cat["status"] != A.CAT_OPEN:
            raise Conflict("报名已截止或抽签已公布，不能再提交报名")
        dup = tx.execute(
            "SELECT id, status FROM registrations WHERE race_id = ? AND id_type = ? AND id_no = ? "
            "AND status NOT IN ('refunded','withdrawn','cancelled','lot_out')",
            (cat["race_id"], id_type, id_no),
        ).fetchone()
        if dup is not None:
            raise Conflict(f"该证件已有有效报名 {dup['id']}（状态 {dup['status']}），不得重复占用")

        reg_id = str(p.get("registration_id") or _new_id("reg")).strip()
        slot = tx.execute(
            "SELECT id, seq FROM slots WHERE category_id = ? AND status = 'free' ORDER BY seq ASC LIMIT 1",
            (category_id,),
        ).fetchone()
        now = utcnow()
        try:
            if slot is not None:
                tx.execute(
                    """INSERT INTO registrations
                       (id, race_id, category_id, id_type, id_no, person_name, status,
                        slot_id, waitlist_seq, fee_fen, version, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?)""",
                    (reg_id, cat["race_id"], category_id, id_type, id_no, person_name,
                     "held", slot["id"], None, cat["fee_fen"], now, now),
                )
                tx.execute("UPDATE slots SET status = 'held', registration_id = ? WHERE id = ?",
                           (reg_id, slot["id"]))
            else:
                row = tx.execute(
                    "SELECT COALESCE(MAX(waitlist_seq), 0) + 1 AS next_seq FROM registrations WHERE category_id = ?",
                    (category_id,),
                ).fetchone()
                wait_seq = int(row["next_seq"])
                tx.execute(
                    """INSERT INTO registrations
                       (id, race_id, category_id, id_type, id_no, person_name, status,
                        slot_id, waitlist_seq, fee_fen, version, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?)""",
                    (reg_id, cat["race_id"], category_id, id_type, id_no, person_name,
                     "waiting", None, wait_seq, cat["fee_fen"], now, now),
                )
        except sqlite3.IntegrityError:
            # 并发下撞同一证件 / 同一名额
            raise Conflict("报名冲突：名额刚被占用或证件已报名")

        if slot is not None:
            self.store.append_event(
                tx, "SlotOccupied", actor=req.actor, race_id=cat["race_id"],
                category_id=category_id, slot_id=slot["id"], registration_id=reg_id,
                payload={"seq": slot["seq"], "id_type": id_type, "id_no": id_no,
                         "person_name": person_name},
            )
            return Result.ok("名额已占用，等待支付", "held", registration_id=reg_id,
                             slot_id=slot["id"], seq=slot["seq"], fee_fen=cat["fee_fen"])
        self.store.append_event(
            tx, "Waitlisted", actor=req.actor, race_id=cat["race_id"],
            category_id=category_id, registration_id=reg_id,
            payload={"waitlist_seq": wait_seq, "id_type": id_type, "id_no": id_no,
                     "person_name": person_name},
        )
        return Result.ok("名额已满，进入候补", "waiting", registration_id=reg_id,
                         waitlist_seq=wait_seq, fee_fen=cat["fee_fen"])

    # ============================================================== 支付
    def _payment_notify(self, tx: sqlite3.Connection, req: Request) -> Result:
        p = req.payload
        notification_id = str(_require(p, "notification_id"))
        # 通知重送：以网关流水号为准，永远只落到原报名
        existing = tx.execute(
            "SELECT registration_id FROM payments WHERE notification_id = ?",
            (notification_id,),
        ).fetchone()
        if existing is not None:
            reg = self._get_registration(tx, existing["registration_id"])
            self.store.append_event(
                tx, "PaymentNotificationReplayed", actor=req.actor,
                race_id=reg["race_id"], category_id=reg["category_id"],
                slot_id=reg["slot_id"], registration_id=reg["id"],
                payload={"notification_id": notification_id},
            )
            return Result.ok("支付通知为重送，仅返回原报名状态", reg["status"],
                             registration_id=reg["id"], status=reg["status"],
                             replayed=True)

        reg_id = str(_require(p, "registration_id"))
        reg = self._get_registration(tx, reg_id)
        amount = int(p.get("amount_fen", reg["fee_fen"]))
        if amount != reg["fee_fen"]:
            raise Conflict(f"支付金额 {amount} 与应付 {reg['fee_fen']} 不一致")
        if reg["status"] == "paid":
            raise Conflict("该报名已付款，不得重复入账")
        if reg["status"] == "waiting":
            raise Conflict("候补记录尚未取得名额，不能付款")
        if reg["status"] != "held":
            raise Conflict(f"当前状态 {reg['status']} 不接受支付")

        payment_id = _new_id("pay")
        now = utcnow()
        tx.execute(
            "INSERT INTO payments (id, notification_id, registration_id, amount_fen, created_at) VALUES (?,?,?,?,?)",
            (payment_id, notification_id, reg_id, amount, now),
        )
        tx.execute(
            "UPDATE registrations SET status = 'paid', version = version + 1, updated_at = ? WHERE id = ?",
            (now, reg_id),
        )
        tx.execute("UPDATE slots SET status = 'confirmed' WHERE id = ?", (reg["slot_id"],))
        self.store.append_event(
            tx, "PaymentReceived", actor=req.actor, race_id=reg["race_id"],
            category_id=reg["category_id"], slot_id=reg["slot_id"], registration_id=reg_id,
            payload={"notification_id": notification_id, "amount_fen": amount},
        )
        return Result.ok("支付已确认，名额锁定", "paid", registration_id=reg_id,
                         slot_id=reg["slot_id"], payment_id=payment_id, amount_fen=amount)

    # ============================================================== 退赛
    def _withdraw(self, tx: sqlite3.Connection, req: Request) -> Result:
        return self._withdraw_or_cancel(tx, req, by_staff=False)

    def _staff_cancel(self, tx: sqlite3.Connection, req: Request) -> Result:
        return self._withdraw_or_cancel(tx, req, by_staff=True)

    def _withdraw_or_cancel(
        self, tx: sqlite3.Connection, req: Request, *, by_staff: bool
    ) -> Result:
        p = req.payload
        reg_id = str(_require(p, "registration_id"))
        reg = self._get_registration(tx, reg_id)
        if by_staff:
            self._require_category_grant(tx, req.actor, reg["category_id"])
        else:
            # 报名人退赛必须出示与记录一致的证件号，防止持报名号的第三人恶意退赛
            id_no = str(_require(p, "id_no"))
            if id_no.strip() != reg["id_no"]:
                raise Forbidden("证件号与报名记录不符，不能代为退赛")
        reason = str(p.get("reason") or ("staff_cancel" if by_staff else "withdraw"))
        event_actor = req.actor
        cat = self._get_category(tx, reg["category_id"])

        status = reg["status"]
        if status in A.TERMINAL_STATES:
            raise Conflict(f"报名已处于终态 {status}，不能再次退赛或取消")

        now = utcnow()
        if status == "waiting":
            tx.execute(
                "UPDATE registrations SET status = 'withdrawn', version = version + 1, updated_at = ? WHERE id = ?",
                (now, reg_id),
            )
            self.store.append_event(
                tx, "WaitlistWithdrawn", actor=event_actor, race_id=reg["race_id"],
                category_id=reg["category_id"], registration_id=reg_id,
                payload={"reason": reason},
            )
            return Result.ok("已退出候补", "withdrawn", registration_id=reg_id)

        if status == "held":
            # 未付款：立即释放并递补
            tx.execute(
                "UPDATE registrations SET status = ?, slot_id = NULL, version = version + 1, updated_at = ? WHERE id = ?",
                ("cancelled" if by_staff else "withdrawn", now, reg_id),
            )
            freed = self._release_slot(tx, reg, reason, event_actor,
                                       "SlotCancelled" if by_staff else "SlotReleased")
            promoted = self._promote_next(tx, cat, event_actor)
            return Result.ok(
                "名额已释放" + ("，已递补下一位候补" if promoted else "，候补为空"),
                "cancelled" if by_staff else "withdrawn",
                registration_id=reg_id, freed_slot_id=freed,
                promoted_registration_id=promoted,
            )

        if status in ("paid", "refund_pending"):
            if status == "refund_pending":
                raise Conflict("退款处理中，请勿重复申请；退款完成后名额自会释放")
            # 已付款：进入退款中，名额暂不释放，直到退款完成通知
            tx.execute(
                "UPDATE registrations SET status = 'refund_pending', version = version + 1, updated_at = ? WHERE id = ?",
                (now, reg_id),
            )
            self.store.append_event(
                tx, "RefundRequested", actor=event_actor, race_id=reg["race_id"],
                category_id=reg["category_id"], slot_id=reg["slot_id"], registration_id=reg_id,
                payload={"reason": reason, "amount_fen": reg["fee_fen"]},
            )
            return Result.ok("退赛已登记，等待退款完成后释放名额", "refund_pending",
                             registration_id=reg_id, slot_id=reg["slot_id"])

        raise Conflict(f"当前状态 {status} 不允许该操作")

    def _refund_notify(self, tx: sqlite3.Connection, req: Request) -> Result:
        p = req.payload
        notification_id = str(_require(p, "notification_id"))
        existing = tx.execute(
            "SELECT registration_id FROM refunds WHERE notification_id = ?",
            (notification_id,),
        ).fetchone()
        if existing is not None:
            reg = self._get_registration(tx, existing["registration_id"])
            self.store.append_event(
                tx, "RefundNotificationReplayed", actor=req.actor,
                race_id=reg["race_id"], category_id=reg["category_id"],
                slot_id=reg["slot_id"], registration_id=reg["id"],
                payload={"notification_id": notification_id},
            )
            return Result.ok("退款通知为重送，名额不会被重复释放", reg["status"],
                             registration_id=reg["id"], status=reg["status"], replayed=True)

        reg_id = str(_require(p, "registration_id"))
        reg = self._get_registration(tx, reg_id)
        amount = int(p.get("amount_fen", reg["fee_fen"]))
        if reg["status"] != "refund_pending":
            raise Conflict(f"当前状态 {reg['status']} 不存在待完成退款")

        now = utcnow()
        tx.execute(
            "INSERT INTO refunds (id, notification_id, registration_id, amount_fen, created_at) VALUES (?,?,?,?,?)",
            (_new_id("ref"), notification_id, reg_id, amount, now),
        )
        tx.execute(
            "UPDATE registrations SET status = 'refunded', slot_id = NULL, version = version + 1, updated_at = ? WHERE id = ?",
            (now, reg_id),
        )
        cat = self._get_category(tx, reg["category_id"])
        freed = self._release_slot(tx, reg, "refund_completed", req.actor, "RefundCompleted")
        promoted: str | None = None
        if cat["status"] in PROMOTABLE_CATEGORY_STATUSES:
            promoted = self._promote_next(tx, cat, req.actor)
        return Result.ok(
            "退款完成，名额已释放" + ("，已递补下一位候补" if promoted else "，未再递补"),
            "refunded", registration_id=reg_id, freed_slot_id=freed,
            promoted_registration_id=promoted, amount_fen=amount,
        )

    # ----------------------------------------------------------- 名额内部
    def _release_slot(
        self, tx: sqlite3.Connection, reg: sqlite3.Row, reason: str, actor: str, event_type: str
    ) -> str:
        slot_id = reg["slot_id"]
        slot = tx.execute("SELECT seq FROM slots WHERE id = ?", (slot_id,)).fetchone()
        tx.execute(
            "UPDATE slots SET status = 'free', registration_id = NULL WHERE id = ?",
            (slot_id,),
        )
        self.store.append_event(
            tx, event_type, actor=actor, race_id=reg["race_id"],
            category_id=reg["category_id"], slot_id=slot_id, registration_id=reg["id"],
            payload={"reason": reason, "seq": slot["seq"] if slot else None},
        )
        # SlotReleased 是统一的“名额可再分配”事实
        if event_type != "SlotReleased":
            self.store.append_event(
                tx, "SlotReleased", actor=actor, race_id=reg["race_id"],
                category_id=reg["category_id"], slot_id=slot_id, registration_id=reg["id"],
                payload={"reason": reason, "seq": slot["seq"] if slot else None},
            )
        return slot_id

    def _promote_next(self, tx: sqlite3.Connection, cat: sqlite3.Row, actor: str) -> str | None:
        """按 waitlist_seq 确定性地把最早候补者补进刚释放的名额。一次只补一人。"""
        if cat["status"] not in PROMOTABLE_CATEGORY_STATUSES:
            return None
        nxt = tx.execute(
            "SELECT * FROM registrations WHERE category_id = ? AND status = 'waiting' "
            "ORDER BY waitlist_seq ASC LIMIT 1",
            (cat["id"],),
        ).fetchone()
        slot = tx.execute(
            "SELECT id, seq FROM slots WHERE category_id = ? AND status = 'free' ORDER BY seq ASC LIMIT 1",
            (cat["id"],),
        ).fetchone()
        if nxt is None or slot is None:
            return None
        now = utcnow()
        tx.execute(
            "UPDATE registrations SET status = 'held', slot_id = ?, version = version + 1, updated_at = ? WHERE id = ?",
            (slot["id"], now, nxt["id"]),
        )
        tx.execute(
            "UPDATE slots SET status = 'held', registration_id = ? WHERE id = ?",
            (nxt["id"], slot["id"]),
        )
        self.store.append_event(
            tx, "SlotOffered", actor=actor, race_id=cat["race_id"],
            category_id=cat["id"], slot_id=slot["id"], registration_id=nxt["id"],
            payload={"seq": slot["seq"], "waitlist_seq": nxt["waitlist_seq"]},
        )
        return str(nxt["id"])

    # ============================================================== 查询
    def _slot_timeline(self, tx: sqlite3.Connection, req: Request) -> Result:
        slot_id = str(_require(req.payload, "slot_id"))
        data = self.store.slot_timeline(slot_id)
        if not data:
            raise NotFound(f"名额不存在: {slot_id}")
        return Result.ok("名额事件时间线", "timeline", **data)

    def _category_list(self, tx: sqlite3.Connection, req: Request) -> Result:
        race_id = req.payload.get("race_id")
        sql = """SELECT c.*,
                        SUM(CASE WHEN r.status IN ('held','paid','refund_pending') THEN 1 ELSE 0 END) AS occupied,
                        SUM(CASE WHEN r.status = 'waiting' THEN 1 ELSE 0 END) AS waiting
                 FROM categories c LEFT JOIN registrations r ON r.category_id = c.id"""
        args: tuple[Any, ...] = ()
        if race_id:
            sql += " WHERE c.race_id = ?"
            args = (str(race_id),)
        sql += " GROUP BY c.id ORDER BY c.id"
        rows = [dict(r) for r in tx.execute(sql, args).fetchall()]
        return Result.ok("组别列表", "listed", categories=rows)

    def _registrant_lookup(self, tx: sqlite3.Connection, req: Request) -> Result:
        p = req.payload
        reg_id = p.get("registration_id")
        if reg_id:
            return Result.ok("报名记录", "found", registration=dict(self._get_registration(tx, str(reg_id))))
        race_id = _require(p, "race_id")
        id_type = _require(p, "id_type")
        id_no = _require(p, "id_no")
        rows = tx.execute(
            "SELECT * FROM registrations WHERE race_id = ? AND id_type = ? AND id_no = ? ORDER BY version, created_at",
            (str(race_id), str(id_type), str(id_no)),
        ).fetchall()
        if not rows:
            raise NotFound("未找到该证件的报名记录")
        return Result.ok("报名记录", "found", registrations=[dict(r) for r in rows])
