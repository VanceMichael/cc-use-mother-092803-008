"""行为测试：围绕北马争议场景验证名额发放、占用、付款、释放与递补。"""
from __future__ import annotations

import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from app import actions as A
from app.contracts import Request
from app.service import MarathonEntryService
from app.store import Store


def req(actor: str, action: str, payload: dict, request_id: str) -> Request:
    return Request(actor, action, payload, request_id)


class MarathonFixture:
    """建好一个赛事 + 全马组别的快速夹具。"""

    def __init__(self, db: str = ":memory:", quota: int = 1, fee: int = 20000) -> None:
        self.svc = MarathonEntryService(db)
        self._doc_of: dict[str, str] = {}
        r = self.svc.handle(req("org", A.EVENT_CREATE, {"name": "北京马拉松"}, "evt"))
        self.race_id = r.data["race_id"]
        r = self.svc.handle(req(
            "org", A.CATEGORY_CREATE,
            {"race_id": self.race_id, "name": "全程马拉松", "fee_fen": fee, "quota": quota},
            "cat-full",
        ))
        self.category_id = r.data["category_id"]

    def batch(self, count: int, request_id: str, actor: str = "org"):
        return self.svc.handle(req(
            actor, A.CATEGORY_OPEN_BATCH,
            {"category_id": self.category_id, "count": count}, request_id,
        ))

    def register(self, id_no: str, request_id: str, name: str | None = None, category_id: str | None = None):
        result = self.svc.handle(req(
            id_no, A.REGISTRANT_REGISTER,
            {"category_id": category_id or self.category_id,
             "id_type": "idcard", "id_no": id_no,
             "person_name": name or f"选手{id_no}"},
            request_id,
        ))
        if result.accepted:
            self._doc_of[result.data["registration_id"]] = id_no
        return result

    def pay(self, reg_id: str, notification_id: str, request_id: str | None = None,
            amount: int | None = None, actor: str = "pay-gateway"):
        return self.svc.handle(req(
            actor, A.PAYMENT_NOTIFY,
            {"registration_id": reg_id, "notification_id": notification_id,
             **({"amount_fen": amount} if amount is not None else {})},
            request_id or f"cmd-pay-{notification_id}",
        ))

    def withdraw(self, reg_id: str, request_id: str, actor: str | None = None,
                 id_no: str | None = None):
        doc = id_no if id_no is not None else self._doc_of.get(reg_id, reg_id)
        return self.svc.handle(req(
            actor or doc, A.REGISTRANT_WITHDRAW,
            {"registration_id": reg_id, "id_no": doc}, request_id,
        ))

    def refund(self, reg_id: str, notification_id: str, request_id: str | None = None):
        return self.svc.handle(req(
            "pay-gateway", A.REFUND_NOTIFY,
            {"registration_id": reg_id, "notification_id": notification_id},
            request_id or f"cmd-ref-{notification_id}",
        ))


class RegistrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = MarathonFixture(quota=10)
        self.fx.batch(1, "batch-1")

    # ---- 场景一：同一证件重复提交不得占用多个名额 ----
    def test_same_document_cannot_hold_multiple_slots(self):
        first = self.fx.register("110101199001011234", "reg-1")
        self.assertTrue(first.accepted)
        self.assertEqual(first.state, "held")

        # 换一个幂等键重复提交，仍然是同一证件 —— 必须拒绝
        second = self.fx.register("110101199001011234", "reg-2")
        self.assertFalse(second.accepted)
        self.assertEqual(second.state, "conflict")

        listing = self.fx.svc.handle(req("org", A.CATEGORY_LIST,
                                         {"race_id": self.fx.race_id}, "list-1"))
        cat = listing.data["categories"][0]
        self.assertEqual(cat["occupied"], 1)

    def test_same_request_id_replays_first_result(self):
        r = req("110101199001011234", A.REGISTRANT_REGISTER,
                {"category_id": self.fx.category_id, "id_type": "idcard",
                 "id_no": "110101199001011234", "person_name": "甲"}, "dup-cmd")
        first = self.fx.svc.handle(r)
        second = self.fx.svc.handle(r)
        self.assertEqual(first, second)

    def test_document_can_re_register_after_refund(self):
        r = self.fx.register("110101199001011234", "reg-1")
        reg_id = r.data["registration_id"]
        self.fx.pay(reg_id, "n-1")
        self.fx.withdraw(reg_id, "w-1")
        self.fx.refund(reg_id, "rn-1")
        # 终态后同一证件可重新报名（名额已交还）
        again = self.fx.register("110101199001011234", "reg-again")
        self.assertTrue(again.accepted)

    # ---- 场景二：支付通知重送只更新原报名 ----
    def test_payment_notification_resend_only_touches_original(self):
        r = self.fx.register("110101199001011234", "reg-1")
        reg_id = r.data["registration_id"]
        slot_id = r.data["slot_id"]

        paid = self.fx.pay(reg_id, "NOTIF-1001")
        self.assertTrue(paid.accepted)
        self.assertEqual(paid.state, "paid")

        # 网关重送：即使是全新 request_id，也凭 notification_id 识别
        replay = self.fx.pay(reg_id, "NOTIF-1001", request_id:="cmd-retry-9999")
        self.assertTrue(replay.accepted)
        self.assertTrue(replay.data["replayed"])
        self.assertEqual(replay.data["registration_id"], reg_id)

        # 没有第二笔支付、名额仍属于原报名
        row = self.fx.svc.store.conn.execute(
            "SELECT COUNT(*) c FROM payments WHERE notification_id = 'NOTIF-1001'"
        ).fetchone()
        self.assertEqual(row["c"], 1)
        slot = self.fx.svc.store.conn.execute(
            "SELECT status, registration_id FROM slots WHERE id = ?", (slot_id,)
        ).fetchone()
        self.assertEqual(slot["status"], "confirmed")
        self.assertEqual(slot["registration_id"], reg_id)

    def test_waitlist_cannot_pay(self):
        fx = MarathonFixture(quota=10)
        fx.batch(1, "b")
        fx.register("A", "r1")
        waiter = fx.register("B", "r2")
        self.assertEqual(waiter.state, "waiting")
        bad = fx.pay(waiter.data["registration_id"], "n-bad")
        self.assertFalse(bad.accepted)

    # ---- 场景三：退赛完成（退款到账）后名额才递补 ----
    def test_paid_slot_moves_to_waitlist_only_after_refund_completed(self):
        fx = MarathonFixture(quota=10)
        fx.batch(1, "b1")
        holder = fx.register("H", "reg-h")
        waiter1 = fx.register("W1", "reg-w1")
        waiter2 = fx.register("W2", "reg-w2")
        self.assertEqual(waiter1.state, "waiting")
        self.assertEqual(waiter2.state, "waiting")

        reg_h = holder.data["registration_id"]
        fx.pay(reg_h, "pay-h")

        fx.withdraw(reg_h, "wd-h")
        # 退款未完成：名额仍是 confirmed，候补不得上移
        self.assertEqual(fx.svc.handle(req(
            "W1", A.REGISTRANT_LOOKUP,
            {"registration_id": waiter1.data["registration_id"]}, "q1"
        )).data["registration"]["status"], "waiting")

        # 重复退赛被拒绝
        again = fx.withdraw(reg_h, "wd-h-2", actor="H")
        self.assertFalse(again.accepted)

        done = fx.refund(reg_h, "ref-h")
        self.assertTrue(done.accepted)
        self.assertEqual(done.state, "refunded")
        self.assertEqual(done.data["promoted_registration_id"], waiter1.data["registration_id"])
        self.assertEqual(done.data["freed_slot_id"], holder.data["slot_id"])

        w1 = fx.svc.handle(req(
            "W1", A.REGISTRANT_LOOKUP,
            {"registration_id": waiter1.data["registration_id"]}, "q2"
        )).data["registration"]
        self.assertEqual(w1["status"], "held")
        self.assertEqual(w1["slot_id"], holder.data["slot_id"])
        # W2 仍在候补
        w2 = fx.svc.handle(req(
            "W2", A.REGISTRANT_LOOKUP,
            {"registration_id": waiter2.data["registration_id"]}, "q3"
        )).data["registration"]
        self.assertEqual(w2["status"], "waiting")

    def test_refund_notification_resend_does_not_double_promote(self):
        fx = MarathonFixture(quota=10)
        fx.batch(1, "b1")
        holder = fx.register("H", "reg-h")
        waiter1 = fx.register("W1", "reg-w1")
        waiter2 = fx.register("W2", "reg-w2")
        fx.pay(holder.data["registration_id"], "p1")
        fx.withdraw(holder.data["registration_id"], "wd1")

        first = fx.refund(holder.data["registration_id"], "REF-1")
        replay = fx.refund(holder.data["registration_id"], "REF-1", request_id="cmd-resend")
        self.assertTrue(replay.data["replayed"])
        # 只递补一次：W2 没有被提升
        self.assertEqual(first.data["promoted_registration_id"], waiter1.data["registration_id"])
        w2 = fx.svc.handle(req(
            "W2", A.REGISTRANT_LOOKUP,
            {"registration_id": waiter2.data["registration_id"]}, "q"
        )).data["registration"]["status"]
        self.assertEqual(w2, "waiting")

    def test_unpaid_holder_release_promotes_immediately(self):
        fx = MarathonFixture(quota=10)
        fx.batch(1, "b1")
        holder = fx.register("H", "reg-h")
        waiter = fx.register("W", "reg-w")
        result = fx.withdraw(holder.data["registration_id"], "wd1")
        self.assertTrue(result.accepted)
        self.assertEqual(result.state, "withdrawn")
        self.assertEqual(result.data["promoted_registration_id"], waiter.data["registration_id"])

    # ---- 分批放号与候补的确定顺序 ----
    def test_batch_release_order_and_quota(self):
        fx = MarathonFixture(quota=3)
        b1 = fx.batch(2, "b1")
        self.assertEqual((b1.data["first_seq"], b1.data["last_seq"]), (1, 2))
        b2 = fx.batch(2, "b2")
        self.assertFalse(b2.accepted)  # 容量 3，已放 2，再放 2 超量
        b3 = fx.batch(1, "b3")
        self.assertTrue(b3.accepted)
        self.assertEqual(b3.data["first_seq"], 3)

        seq_rows = [r["seq"] for r in fx.svc.store.conn.execute(
            "SELECT seq FROM slots ORDER BY seq").fetchall()]
        self.assertEqual(seq_rows, [1, 2, 3])

    def test_waitlist_is_fifo(self):
        fx = MarathonFixture(quota=10)
        fx.batch(1, "b")
        fx.register("H", "rh")
        w1 = fx.register("W1", "rw1")
        w2 = fx.register("W2", "rw2")
        self.assertEqual(w1.data["waitlist_seq"], 1)
        self.assertEqual(w2.data["waitlist_seq"], 2)

    # ---- 截止 / 抽签后状态不能倒退 ----
    def test_registration_close_blocks_new_entries_but_promotion_continues(self):
        fx = MarathonFixture(quota=10)
        fx.batch(1, "b")
        holder = fx.register("H", "rh")
        waiter = fx.register("W", "rw")
        fx.pay(holder.data["registration_id"], "p")

        closed = fx.svc.handle(req(
            "org", A.EVENT_CLOSE_REGISTRATION, {"race_id": fx.race_id}, "close-1"))
        self.assertTrue(closed.accepted)

        late = fx.register("LATE", "rl")
        self.assertFalse(late.accepted)

        # 截止后已付款选手退赛，退款完成仍可递补在册候补
        fx.withdraw(holder.data["registration_id"], "wd")
        done = fx.refund(holder.data["registration_id"], "rf")
        self.assertEqual(done.data["promoted_registration_id"], waiter.data["registration_id"])

        # 截止后不能再放号，也不能重新开放
        self.assertFalse(fx.batch(1, "b2").accepted)
        self.assertFalse(fx.svc.handle(req(
            "org", A.EVENT_CLOSE_REGISTRATION, {"race_id": fx.race_id}, "close-2")).accepted)

    def test_lottery_publish_is_terminal_and_cannot_roll_back(self):
        fx = MarathonFixture(quota=10)
        fx.batch(1, "b")
        holder = fx.register("H", "rh")
        fx.pay(holder.data["registration_id"], "p")
        waiter = fx.register("W", "rw")

        published = fx.svc.handle(req(
            "org", A.EVENT_PUBLISH_LOTTERY, {"race_id": fx.race_id}, "lot-1"))
        self.assertTrue(published.accepted)
        self.assertEqual(published.data["lot_out"], 1)

        # 候补落选举且不可回退
        w = fx.svc.handle(req("W", A.REGISTRANT_LOOKUP,
                              {"registration_id": waiter.data["registration_id"]}, "q")
                          ).data["registration"]
        self.assertEqual(w["status"], "lot_out")
        # 已付款者不受影响
        h = fx.svc.handle(req("H", A.REGISTRANT_LOOKUP,
                              {"registration_id": holder.data["registration_id"]}, "q2")
                          ).data["registration"]
        self.assertEqual(h["status"], "paid")

        # 重复公布、再放号、新报名全部被拒
        self.assertFalse(fx.svc.handle(req(
            "org", A.EVENT_PUBLISH_LOTTERY, {"race_id": fx.race_id}, "lot-2")).accepted)
        self.assertFalse(fx.batch(1, "b2").accepted)
        self.assertFalse(fx.register("X", "rx").accepted)

        # 抽签后退款释放的名额不再流向已落选候补
        fx.withdraw(holder.data["registration_id"], "wd", actor="H")
        done = fx.refund(holder.data["registration_id"], "rf")
        self.assertIsNone(done.data["promoted_registration_id"])

    # ---- 工作人员授权 ----
    def test_staff_can_only_operate_granted_categories(self):
        fx = MarathonFixture(quota=10)
        fx.batch(1, "b-staff")  # org 自带权限
        holder = fx.register("H", "rh")

        # 未授权工作人员不能取消报名
        denied = fx.svc.handle(req(
            "staff-7", A.STAFF_CANCEL,
            {"registration_id": holder.data["registration_id"]}, "cancel-1"))
        self.assertEqual(denied.state, "forbidden")

        # 也不能放号
        self.assertFalse(fx.batch(1, "b-x", actor="staff-7").accepted)

        granted = fx.svc.handle(req(
            "org", A.CATEGORY_GRANT,
            {"category_id": fx.category_id, "staff_id": "staff-7"}, "grant-1"))
        self.assertTrue(granted.accepted)

        ok = fx.svc.handle(req(
            "staff-7", A.STAFF_CANCEL,
            {"registration_id": holder.data["registration_id"]}, "cancel-2"))
        self.assertTrue(ok.accepted)

        fx.svc.handle(req("org", A.CATEGORY_REVOKE,
                          {"category_id": fx.category_id, "staff_id": "staff-7"}, "revoke-1"))
        # 取消后释放的名额被下一位选手占用；staff-7 已无权再操作该组别
        next_runner = fx.register("H2", "rh2")
        again = fx.svc.handle(req(
            "staff-7", A.STAFF_CANCEL,
            {"registration_id": next_runner.data["registration_id"]}, "cancel-3"))
        self.assertEqual(again.state, "forbidden")

    def test_non_admin_cannot_manage_race(self):
        r = self.fx.svc.handle(req(
            "someone", A.EVENT_CLOSE_REGISTRATION,
            {"race_id": self.fx.race_id}, "close-x"))
        self.assertEqual(r.state, "forbidden")

    # ---- 名额事件还原 ----
    def test_slot_timeline_reconstructs_full_lifecycle(self):
        fx = MarathonFixture(quota=10)
        fx.batch(1, "b")
        holder = fx.register("H", "rh")
        slot_id = holder.data["slot_id"]
        fx.register("W", "rw")
        fx.pay(holder.data["registration_id"], "PAY-1")
        fx.withdraw(holder.data["registration_id"], "WD-1", actor="H")
        fx.refund(holder.data["registration_id"], "REF-1")

        tl = fx.svc.handle(req("org", A.SLOT_TIMELINE, {"slot_id": slot_id}, "tl-1"))
        self.assertTrue(tl.accepted)
        types = [e["event_type"] for e in tl.data["events"]]
        self.assertEqual(types, [
            "SlotIssued",        # 放号
            "SlotOccupied",      # 占用
            "PaymentReceived",   # 付款
            "RefundRequested",   # 申请退赛（退款中）
            "RefundCompleted",   # 退款完成
            "SlotReleased",      # 释放
            "SlotOffered",       # 递补给候补
        ])
        # 事件顺序单调，占用与递补指向不同报名
        seqs = [e["seq"] for e in tl.data["events"]]
        self.assertEqual(seqs, sorted(seqs))
        occupied = next(e for e in tl.data["events"] if e["event_type"] == "SlotOccupied")
        offered = next(e for e in tl.data["events"] if e["event_type"] == "SlotOffered")
        self.assertNotEqual(occupied["registration_id"], offered["registration_id"])

    # ---- 跨连接并发：同一证件抢名额只有一个赢家 ----
    def test_concurrent_same_document_only_one_holder(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "race.db")
            fx = MarathonFixture(path, quota=10)
            fx.batch(1, "b")

            def attempt(idx: int):
                svc = MarathonEntryService(path)
                try:
                    return svc.handle(req(
                        f"DUP", A.REGISTRANT_REGISTER,
                        {"category_id": fx.category_id, "id_type": "idcard",
                         "id_no": "DUP-DOC", "person_name": f"选手{idx}"},
                        f"conc-{idx}")).accepted
                finally:
                    svc.close()

            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(attempt, range(8)))
            self.assertEqual(sum(results), 1)
            fx.svc.close()


if __name__ == "__main__":
    unittest.main()
