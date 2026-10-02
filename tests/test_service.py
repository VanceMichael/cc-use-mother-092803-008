"""赛事报名后端的行为测试。

每个测试类对应题面中的一条硬要求：
* 同证件重复提交不得占多个名额；
* 分批放号与候补递补遵循确定顺序；
* 支付/退款通知重送只更新原报名、绝不再占名额；
* 退赛完成后名额才交给下一位候补；
* 报名截止与抽签公布后状态不可倒退；
* 工作人员只能操作获授权组别；
* 争议接口可还原指定名额的全部事件；
* SQLite 落盘后重放历史、命令幂等依然成立。
"""
import os
import tempfile
import threading
import unittest
from datetime import datetime, timezone

from app.contracts import Request
from app.service import MarathonEntryService
from app.store import EventStore


def req(actor, action, payload, request_id):
    return Request(actor, action, payload, request_id,
                   datetime.now(timezone.utc))


def call(service, actor, action, payload, request_id):
    return service.handle(req(actor, action, payload, request_id))


class ServiceFixture:
    """搭建：赛事 bj2026 + 全马组(full) + 半马组(half) + 分组授权工作人员。"""

    def __init__(self, store=None):
        self.service = MarathonEntryService(store or EventStore(":memory:"))
        self.seq = 0
        self.admin = "admin-1"
        self.staff = "staff-full"
        self.staff2 = "staff-half"
        self.do(self.admin, "create_race", {"race_id": "bj2026", "name": "北京马拉松"})
        self.do(self.admin, "add_group", {
            "race_id": "bj2026", "group_id": "full", "name": "全程马拉松",
            "capacity": 2, "fee": 20000, "mode": "fcfs",
        })
        self.do(self.admin, "add_group", {
            "race_id": "bj2026", "group_id": "half", "name": "半程马拉松",
            "capacity": 1, "fee": 12000, "mode": "fcfs",
        })
        self.do(self.admin, "authorize_staff",
                {"race_id": "bj2026", "staff_id": self.staff, "group_ids": ["full"]})
        self.do(self.admin, "authorize_staff",
                {"race_id": "bj2026", "staff_id": self.staff2, "group_ids": ["half"]})
        self.do(self.admin, "open_registration", {"race_id": "bj2026"})

    def do(self, actor, action, payload):
        self.seq += 1
        return self.service.handle(
            req(actor, action, payload, f"{action}-{self.seq}"))

    def close(self):
        self.service.close()


def registrant(id_no, name=None):
    return {"id_type": "idcard", "id_no": id_no, "name": name or f"选手{id_no[-4:]}"}


def event_types(result):
    return [e["event_type"] for e in result.data["events"]]


class DuplicateSubmissionTest(unittest.TestCase):
    def setUp(self):
        self.fx = ServiceFixture()
        self.s = self.fx.service

    def tearDown(self):
        self.fx.close()

    def test_same_id_cannot_hold_two_slots(self):
        """同一证件重复提交：只能有一个报名、一个占用。"""
        r1 = call(self.s, "110101", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("110101"), "client_token": "t-1",
        }, "reg-1")
        self.assertTrue(r1.accepted, r1.message)

        # 同一提交令牌（连续点击）→ 合并到原报名
        r1b = call(self.s, "110101", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("110101"), "client_token": "t-1",
        }, "reg-1b")
        self.assertFalse(r1b.accepted)
        self.assertEqual(r1b.state, "already_registered")
        self.assertEqual(r1b.data["entry_id"], r1.data["events"][0]["data"]["entry_id"])

        # 换新令牌再交一次 → 仍然拒绝
        r1c = call(self.s, "110101", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("110101"), "client_token": "t-2",
        }, "reg-1c")
        self.assertFalse(r1c.accepted)
        self.assertEqual(r1c.state, "duplicate_registration")

        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q-1")
        self.assertEqual(len(race.data["entries"]), 1)

        # 即便名额放出，该证件也只能占一个；多余名额因无候补而空闲
        self.fx.do(self.fx.staff, "release_quota",
                   {"race_id": "bj2026", "group_id": "full", "count": 2, "batch_id": "b1"})
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q-2")
        self.assertEqual(race.data["groups"]["full"]["held"], 1)
        self.assertEqual(race.data["groups"]["full"]["waitlist"], [])

    def test_command_idempotency_same_request_id(self):
        r1 = call(self.s, "110102", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("110102"),
        }, "same-rid")
        r2 = call(self.s, "110102", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("110102"),
        }, "same-rid")
        self.assertEqual(r1, r2)
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q")
        self.assertEqual(len(race.data["entries"]), 1)

    def test_concurrent_duplicate_registrations(self):
        """真实并发下多个相同证件的报名请求，只能赢一个。"""
        outcomes = []

        def worker(rid):
            r = call(self.s, "110103", "register", {
                "race_id": "bj2026", "group_id": "full",
                "registrant": registrant("110103"), "client_token": rid,
            }, rid)
            outcomes.append(r.accepted)

        threads = [threading.Thread(target=worker, args=(f"c{i}",)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(outcomes.count(True), 1)
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q")
        self.assertEqual(len(race.data["entries"]), 1)


class QuotaAndWaitlistTest(unittest.TestCase):
    def setUp(self):
        self.fx = ServiceFixture()
        self.s = self.fx.service
        # 先收 4 个报名（容量 2），此时一个名额都没放 → 全部候补，顺序确定
        self.entry_ids = []
        for i, id_no in enumerate(["2201", "2202", "2203", "2204"]):
            r = call(self.s, id_no, "register", {
                "race_id": "bj2026", "group_id": "full",
                "registrant": registrant(id_no),
            }, f"reg-{i}")
            self.assertTrue(r.accepted)
            self.entry_ids.append(r.data["events"][0]["data"]["entry_id"])

    def tearDown(self):
        self.fx.close()

    def test_waitlist_is_registration_order(self):
        r = call(self.s, self.fx.admin, "get_waitlist",
                 {"race_id": "bj2026", "group_id": "full"}, "wl-0")
        self.assertEqual([e["entry_id"] for e in r.data["waitlist"]], self.entry_ids)

    def test_batch_release_fills_in_deterministic_order(self):
        """分批放号：第一批 1 个 → 队首占用；第二批 1 个 → 第二位占用。"""
        r = self.fx.do(self.fx.staff, "release_quota",
                       {"race_id": "bj2026", "group_id": "full", "count": 1, "batch_id": "b1"})
        self.assertTrue(r.accepted)
        held = [e["data"] for e in r.data["events"] if e["event_type"] == "SlotHeld"]
        self.assertEqual(held[0]["entry_id"], self.entry_ids[0])
        self.assertEqual(held[0]["slot_id"], "full-S0001")

        self.fx.do(self.fx.staff, "release_quota",
                   {"race_id": "bj2026", "group_id": "full", "count": 1, "batch_id": "b2"})
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q")
        g = race.data["groups"]["full"]
        self.assertEqual(g["issued"], 2)
        self.assertEqual(g["held"], 2)
        self.assertEqual(g["waitlist"], self.entry_ids[2:])

    def test_release_cannot_exceed_capacity_or_repeat_batch(self):
        r = self.fx.do(self.fx.staff, "release_quota",
                       {"race_id": "bj2026", "group_id": "full", "count": 3, "batch_id": "b1"})
        self.assertFalse(r.accepted)
        self.assertIn("容量", r.message)
        self.fx.do(self.fx.staff, "release_quota",
                   {"race_id": "bj2026", "group_id": "full", "count": 2, "batch_id": "b1"})
        again = self.fx.do(self.fx.staff, "release_quota",
                           {"race_id": "bj2026", "group_id": "full", "count": 1, "batch_id": "b1"})
        self.assertFalse(again.accepted)
        self.assertIn("b1", again.message)

    def test_new_registration_takes_free_slot_without_jumping_queue(self):
        # 放出 2 个，前两位占用；再来新人：无空名额，只能候补队尾
        self.fx.do(self.fx.staff, "release_quota",
                   {"race_id": "bj2026", "group_id": "full", "count": 2, "batch_id": "b1"})
        call(self.s, "2205", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("2205"),
        }, "reg-late")
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q")
        self.assertEqual(race.data["groups"]["full"]["waitlist"][0], self.entry_ids[2])

    def test_direct_hold_when_slot_already_free(self):
        # 全新赛事（无候补）：先放号空闲，再报名 → 直接占用，不走候补
        fx = ServiceFixture()
        fx.do(fx.staff, "release_quota",
              {"race_id": "bj2026", "group_id": "full", "count": 1, "batch_id": "b1"})
        r = call(fx.service, "2209", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("2209"),
        }, "reg-direct")
        self.assertEqual(event_types(r), ["EntryRegistered", "SlotHeld"])
        self.assertEqual(r.data["events"][1]["data"]["mode"], "direct")
        fx.close()

    def test_waiting_entry_cannot_pay(self):
        """候补者没有占用名额，支付通知必须拒绝。"""
        r = call(self.s, "payment:wechat", "payment_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "n-wait",
            "payment_id": f"PAY-{self.entry_ids[0]}", "amount": 20000,
        }, "pay-wait")
        self.assertFalse(r.accepted)


class PaymentRefundAndPromotionTest(unittest.TestCase):
    def setUp(self):
        self.fx = ServiceFixture()
        self.s = self.fx.service
        self.ids = []
        for id_no in ["3301", "3302", "3303"]:
            r = call(self.s, id_no, "register", {
                "race_id": "bj2026", "group_id": "full",
                "registrant": registrant(id_no),
            }, f"reg-{id_no}")
            self.ids.append(r.data["events"][0]["data"]["entry_id"])
        self.fx.do(self.fx.staff, "release_quota",
                   {"race_id": "bj2026", "group_id": "full", "count": 2, "batch_id": "b1"})
        paid = call(self.s, "payment:wechat", "payment_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "W-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "pay-1")
        self.assertTrue(paid.accepted)

    def tearDown(self):
        self.fx.close()

    def test_payment_notice_replay_updates_nothing(self):
        """支付平台重送同一通知：只认原报名，不新增事件、不重复占用。"""
        before = self.fx.service._store.load_events("bj2026")
        replay = call(self.s, "payment:wechat", "payment_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "W-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "pay-1-replay")
        self.assertTrue(replay.accepted)
        self.assertTrue(replay.data["replayed"])
        self.assertEqual(replay.data["entry_id"], self.ids[0])
        after = self.fx.service._store.load_events("bj2026")
        self.assertEqual(len(before), len(after))

        # 换一个通知号再付同一笔 → 已付款，拒绝
        second = call(self.s, "payment:wechat", "payment_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "W-2",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "pay-2")
        self.assertFalse(second.accepted)
        self.assertEqual(second.state, "paid")

    def test_refund_frees_slot_and_promotes_next_waitlister(self):
        """退款完成 → 名额释放 → 队首候补(3303)立即递补占用。"""
        refund = call(self.s, "payment:wechat", "refund_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "R-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "ref-1")
        self.assertTrue(refund.accepted)
        self.assertEqual(event_types(refund),
                         ["PaymentRefunded", "SlotFreed", "EntryTerminated", "SlotHeld"])
        hold = refund.data["events"][-1]["data"]
        self.assertEqual(hold["entry_id"], self.ids[2])
        self.assertEqual(hold["slot_id"], "full-S0001")

        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q")
        g = race.data["groups"]
        self.assertEqual(g["full"]["held"], 2)
        self.assertEqual(g["full"]["waitlist"], [])
        self.assertEqual(race.data["entries"][self.ids[0]]["phase"], "terminated")
        self.assertIsNone(race.data["entries"][self.ids[0]]["slot_id"])

    def test_refund_replay_does_not_reoccupy(self):
        """题面事故：退款重放不能让已交出的席位被错误再次占用。"""
        call(self.s, "payment:wechat", "refund_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "R-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "ref-1")
        before = self.fx.service._store.load_events("bj2026")

        replay = call(self.s, "payment:wechat", "refund_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "R-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "ref-1-replay")
        self.assertTrue(replay.data.get("replayed"))

        # 不同通知号、同一已退支付单再来一次：拒绝
        again = call(self.s, "payment:alipay", "refund_notice", {
            "race_id": "bj2026", "channel": "alipay", "notice_id": "R-2",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "ref-2")
        self.assertFalse(again.accepted)
        self.assertEqual(again.state, "refunded")

        after = self.fx.service._store.load_events("bj2026")
        self.assertEqual(len(before), len(after))
        # 递补者仍稳稳持有该名额
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q")
        self.assertEqual(race.data["entries"][self.ids[2]]["slot_id"], "full-S0001")

    def test_late_payment_after_termination_is_rejected(self):
        # 退赛完成 + 退款结清，名额已给候补；此后迟到的付款通知不得复活旧报名
        call(self.s, "payment:wechat", "refund_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "R-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "ref-1")
        late = call(self.s, "payment:bank", "payment_notice", {
            "race_id": "bj2026", "channel": "bank", "notice_id": "LATE-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "late-pay")
        self.assertFalse(late.accepted)
        self.assertEqual(late.state, "terminated")

    def test_wrong_amount_notice_rejected(self):
        bad = call(self.s, "payment:wechat", "payment_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "W-9",
            "payment_id": f"PAY-{self.ids[1]}", "amount": 10,
        }, "pay-bad")
        self.assertFalse(bad.accepted)

    def test_notice_actor_must_match_channel(self):
        bad = call(self.s, "payment:alipay", "payment_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "W-X",
            "payment_id": f"PAY-{self.ids[1]}", "amount": 20000,
        }, "pay-forged")
        self.assertFalse(bad.accepted)
        self.assertEqual(bad.state, "forbidden")


class WithdrawalFlowTest(unittest.TestCase):
    def setUp(self):
        self.fx = ServiceFixture()
        self.s = self.fx.service
        self.ids = []
        for id_no in ["4401", "4402", "4403"]:
            r = call(self.s, id_no, "register", {
                "race_id": "bj2026", "group_id": "full",
                "registrant": registrant(id_no),
            }, f"reg-{id_no}")
            self.ids.append(r.data["events"][0]["data"]["entry_id"])
        self.fx.do(self.fx.staff, "release_quota",
                   {"race_id": "bj2026", "group_id": "full", "count": 1, "batch_id": "b1"})
        call(self.s, "payment:wechat", "payment_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "W-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "pay-1")

    def tearDown(self):
        self.fx.close()

    def test_slot_moves_only_after_withdrawal_completed(self):
        # 选手申请退赛：仅登记，名额不释放，候补不前进
        ask = call(self.s, "4401", "request_withdrawal",
                   {"race_id": "bj2026", "entry_id": self.ids[0]}, "wd-ask")
        self.assertTrue(ask.accepted)
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q1")
        self.assertEqual(race.data["entries"][self.ids[0]]["phase"], "withdrawing")
        self.assertEqual(race.data["entries"][self.ids[0]]["slot_id"], "full-S0001")
        self.assertEqual(race.data["groups"]["full"]["waitlist"][0], self.ids[1])

        # 退赛中不能重复申请
        again = call(self.s, "4401", "request_withdrawal",
                     {"race_id": "bj2026", "entry_id": self.ids[0]}, "wd-ask-2")
        self.assertFalse(again.accepted)

        # 工作人员完成退赛 → 此刻名额才交给队首 4402；退款挂账（refund_due）
        done = call(self.s, self.fx.staff, "complete_withdrawal",
                    {"race_id": "bj2026", "entry_id": self.ids[0]}, "wd-done")
        self.assertTrue(done.accepted)
        self.assertEqual(event_types(done), ["SlotFreed", "EntryTerminated", "SlotHeld"])
        self.assertEqual(done.data["events"][-1]["data"]["entry_id"], self.ids[1])

        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q2")
        terminated = race.data["entries"][self.ids[0]]
        self.assertTrue(terminated["refund_due"])
        self.assertFalse(terminated["refunded"])

        # 退款通知后到：只结清款项，绝不再动名额
        refund = call(self.s, "payment:wechat", "refund_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "R-9",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "ref-late")
        self.assertTrue(refund.accepted)
        self.assertEqual(event_types(refund), ["PaymentRefunded"])
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q3")
        self.assertTrue(race.data["entries"][self.ids[0]]["refunded"])
        self.assertEqual(race.data["entries"][self.ids[1]]["slot_id"], "full-S0001")

    def test_waitlister_withdrawing_never_touches_slots(self):
        # 候补者 4402 退赛：直接终止出队，4403 升到候补队首；无 SlotFreed
        r = call(self.s, "4402", "request_withdrawal",
                 {"race_id": "bj2026", "entry_id": self.ids[1]}, "wd-wait")
        self.assertEqual(event_types(r), ["EntryTerminated"])
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q")
        self.assertEqual(race.data["groups"]["full"]["waitlist"], [self.ids[2]])


class PhaseGuardTest(unittest.TestCase):
    def setUp(self):
        self.fx = ServiceFixture()
        self.s = self.fx.service

    def tearDown(self):
        self.fx.close()

    def test_registration_close_is_one_way(self):
        self.assertTrue(self.fx.do(self.fx.admin, "close_registration",
                                   {"race_id": "bj2026"}).accepted)
        # 截止后报名被拒
        r = call(self.s, "5501", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("5501"),
        }, "late-reg")
        self.assertFalse(r.accepted)
        # 不能重新开放
        reopen = self.fx.do(self.fx.admin, "open_registration", {"race_id": "bj2026"})
        self.assertFalse(reopen.accepted)
        # 截止后不能再放号
        no_quota = self.fx.do(self.fx.staff, "release_quota",
                              {"race_id": "bj2026", "group_id": "full",
                               "count": 1, "batch_id": "bx"})
        self.assertFalse(no_quota.accepted)

    def test_actions_before_setup_rejected(self):
        # 未开放报名时不能报名
        fx = ServiceFixture()
        fx.do(fx.admin, "create_race", {"race_id": "r2", "name": "另一场"})
        fx.do(fx.admin, "add_group", {
            "race_id": "r2", "group_id": "g", "name": "g",
            "capacity": 1, "fee": 1, "mode": "fcfs",
        })
        r = call(fx.service, "5502", "register", {
            "race_id": "r2", "group_id": "g",
            "registrant": registrant("5502"),
        }, "early-reg")
        self.assertFalse(r.accepted)
        fx.close()


class StaffAuthorizationTest(unittest.TestCase):
    def setUp(self):
        self.fx = ServiceFixture()
        self.s = self.fx.service

    def tearDown(self):
        self.fx.close()

    def test_staff_scoped_to_authorized_group(self):
        ok = self.fx.do(self.fx.staff, "release_quota",
                        {"race_id": "bj2026", "group_id": "full",
                         "count": 1, "batch_id": "b1"})
        self.assertTrue(ok.accepted)
        denied = self.fx.do(self.fx.staff, "release_quota",
                            {"race_id": "bj2026", "group_id": "half",
                             "count": 1, "batch_id": "b1"})
        self.assertFalse(denied.accepted)
        self.assertEqual(denied.state, "forbidden")

        # half 组只能由 staff-half 放号
        ok2 = self.fx.do(self.fx.staff2, "release_quota",
                         {"race_id": "bj2026", "group_id": "half",
                          "count": 1, "batch_id": "h1"})
        self.assertTrue(ok2.accepted)

        # 陌生人不是工作人员
        stranger = self.fx.do("nobody", "release_quota",
                              {"race_id": "bj2026", "group_id": "full",
                               "count": 1, "batch_id": "b2"})
        self.assertEqual(stranger.state, "forbidden")

    def test_complete_withdrawal_requires_group_scope(self):
        # 选手在 full 组占用名额并进入退赛中，staff-half 无权完成
        r = call(self.s, "6602", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("6602"),
        }, "reg-full")
        entry_id = r.data["events"][0]["data"]["entry_id"]
        self.fx.do(self.fx.staff, "release_quota",
                   {"race_id": "bj2026", "group_id": "full", "count": 1, "batch_id": "b1"})
        call(self.s, "6602", "request_withdrawal",
             {"race_id": "bj2026", "entry_id": entry_id}, "wd")
        denied = call(self.s, self.fx.staff2, "complete_withdrawal",
                      {"race_id": "bj2026", "entry_id": entry_id}, "wd-denied")
        self.assertEqual(denied.state, "forbidden")
        ok = call(self.s, self.fx.staff, "complete_withdrawal",
                  {"race_id": "bj2026", "entry_id": entry_id}, "wd-ok")
        self.assertTrue(ok.accepted)

    def test_non_admin_cannot_manage_race(self):
        for action, payload in [
            ("add_group", {"race_id": "bj2026", "group_id": "x", "name": "x",
                           "capacity": 1, "fee": 1, "mode": "fcfs"}),
            ("close_registration", {"race_id": "bj2026"}),
            ("authorize_staff", {"race_id": "bj2026", "staff_id": "z", "group_ids": ["*"]}),
        ]:
            r = self.fx.do(self.fx.staff, action, payload)
            self.assertFalse(r.accepted, action)


class SlotHistoryTest(unittest.TestCase):
    def setUp(self):
        self.fx = ServiceFixture()
        self.s = self.fx.service
        self.ids = []
        for id_no in ["7701", "7702"]:
            r = call(self.s, id_no, "register", {
                "race_id": "bj2026", "group_id": "full",
                "registrant": registrant(id_no),
            }, f"reg-{id_no}")
            self.ids.append(r.data["events"][0]["data"]["entry_id"])
        self.fx.do(self.fx.staff, "release_quota",
                   {"race_id": "bj2026", "group_id": "full", "count": 1, "batch_id": "b1"})
        call(self.s, "payment:wechat", "payment_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "W-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "pay-1")
        call(self.s, "payment:wechat", "refund_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "R-1",
            "payment_id": f"PAY-{self.ids[0]}", "amount": 20000,
        }, "ref-1")

    def tearDown(self):
        self.fx.close()

    def test_slot_timeline_reconstructs_full_lifecycle(self):
        """争议接口：还原 full-S0001 发放→占用→付款→退款释放→递补占用全过程。"""
        r = call(self.s, self.fx.admin, "slot_history",
                 {"race_id": "bj2026", "slot_id": "full-S0001"}, "hist")
        self.assertTrue(r.accepted)
        timeline = r.data["timeline"]
        types = [e["event_type"] for e in timeline]
        self.assertEqual(types, [
            "QuotaIssued", "SlotHeld", "PaymentRecorded",
            "PaymentRefunded", "SlotFreed", "SlotHeld",
        ])
        # 每个事件都带有序号、操作人、时间，可独立举证
        for e in timeline:
            self.assertTrue(e["event_id"])
            self.assertIn("stream_seq", e)
            self.assertTrue(e["occurred_at"])
        self.assertEqual(timeline[1]["data"]["entry_id"], self.ids[0])
        self.assertEqual(timeline[-1]["data"]["entry_id"], self.ids[1])
        seqs = [e["stream_seq"] for e in timeline]
        self.assertEqual(seqs, sorted(seqs))

    def test_entry_history_available_to_owner_and_staff(self):
        r = call(self.s, "7702", "entry_history",
                 {"race_id": "bj2026", "entry_id": self.ids[1]}, "eh-owner")
        self.assertTrue(r.accepted)
        denied = call(self.s, "9999", "entry_history",
                      {"race_id": "bj2026", "entry_id": self.ids[1]}, "eh-stranger")
        self.assertEqual(denied.state, "forbidden")
        staff = call(self.s, self.fx.staff, "entry_history",
                     {"race_id": "bj2026", "entry_id": self.ids[1]}, "eh-staff")
        self.assertTrue(staff.accepted)


class LotteryTest(unittest.TestCase):
    def setUp(self):
        self.fx = ServiceFixture()
        self.s = self.fx.service
        self.fx.do(self.fx.admin, "add_group", {
            "race_id": "bj2026", "group_id": "lucky", "name": "慈善抽签",
            "capacity": 2, "fee": 50000, "mode": "lottery",
        })
        self.ids = []
        for id_no in ["8801", "8802", "8803", "8804"]:
            r = call(self.s, id_no, "register", {
                "race_id": "bj2026", "group_id": "lucky",
                "registrant": registrant(id_no),
            }, f"reg-{id_no}")
            self.ids.append(r.data["events"][0]["data"]["entry_id"])
        self.fx.do(self.fx.admin, "close_registration", {"race_id": "bj2026"})

    def tearDown(self):
        self.fx.close()

    def test_lottery_deterministic_and_publication_irreversible(self):
        drawn = self.fx.do(self.fx.admin, "run_lottery",
                           {"race_id": "bj2026", "seed": 20261002})
        self.assertTrue(drawn.accepted)
        # 未公布前不占名额
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q0")
        self.assertEqual(race.data["groups"]["lucky"]["held"], 0)

        # 重抽被拒（抽签结果不可变）
        redraw = self.fx.do(self.fx.admin, "run_lottery",
                            {"race_id": "bj2026", "seed": 999})
        self.assertFalse(redraw.accepted)

        published = self.fx.do(self.fx.admin, "publish_lottery", {"race_id": "bj2026"})
        self.assertTrue(published.accepted)
        draw_event = [e for e in drawn.data["events"]
                      if e["event_type"] == "LotteryDrawn"
                      and e["data"]["group_id"] == "lucky"][0]
        winners = draw_event["data"]["ordering"][:2]
        holds = [e["data"]["entry_id"] for e in published.data["events"]
                 if e["event_type"] == "SlotHeld"]
        self.assertEqual(holds, winners)

        # 公布后阶段为 published，不能再抽签/截止/开放
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q1")
        self.assertEqual(race.data["phase"], "published")
        for action in ["run_lottery", "close_registration", "open_registration"]:
            r = self.fx.do(self.fx.admin, action, {"race_id": "bj2026", "seed": 1})
            self.assertFalse(r.accepted, action)

    def test_lottery_promotion_follows_draw_rank(self):
        self.fx.do(self.fx.admin, "run_lottery", {"race_id": "bj2026", "seed": 42})
        pre = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "qp")
        order = pre.data["groups"]["lucky"]["lottery_order"]
        self.fx.do(self.fx.admin, "publish_lottery", {"race_id": "bj2026"})
        # 中签队首付款后退赛退款，名额按抽签排名交给第 3 名
        first = order[0]
        call(self.s, "payment:wechat", "payment_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "LW-1",
            "payment_id": f"PAY-{first}", "amount": 50000,
        }, "lpay")
        call(self.s, "payment:wechat", "refund_notice", {
            "race_id": "bj2026", "channel": "wechat", "notice_id": "LR-1",
            "payment_id": f"PAY-{first}", "amount": 50000,
        }, "lref")
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "q")
        promoted = race.data["entries"][order[2]]
        self.assertEqual(promoted["slot_id"], "lucky-S0001")
        self.assertEqual(promoted["phase"], "held")


class RaceVisibilityTest(unittest.TestCase):
    def setUp(self):
        self.fx = ServiceFixture()
        self.s = self.fx.service
        call(self.s, "1001", "register", {
            "race_id": "bj2026", "group_id": "full",
            "registrant": registrant("1001"),
        }, "reg-full")
        call(self.s, "1002", "register", {
            "race_id": "bj2026", "group_id": "half",
            "registrant": registrant("1002"),
        }, "reg-half")

    def tearDown(self):
        self.fx.close()

    def test_race_overview_scoped_and_forbidden(self):
        stranger = call(self.s, "nobody", "get_race", {"race_id": "bj2026"}, "g0")
        self.assertFalse(stranger.accepted)
        self.assertEqual(stranger.state, "forbidden")

        scoped = call(self.s, self.fx.staff, "get_race", {"race_id": "bj2026"}, "g1")
        self.assertTrue(scoped.accepted)
        self.assertEqual(set(scoped.data["groups"].keys()), {"full"})
        self.assertEqual(set(scoped.data["entries"].keys()), {"full-E0001"})

        admin = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "g2")
        self.assertEqual(set(admin.data["groups"].keys()), {"full", "half"})

    def test_paid_count_excludes_refunded_entries(self):
        self.fx.do(self.fx.staff, "release_quota",
                   {"race_id": "bj2026", "group_id": "full", "count": 1, "batch_id": "b1"})
        call(self.s, "payment:wx", "payment_notice", {
            "race_id": "bj2026", "channel": "wx", "notice_id": "W",
            "payment_id": "PAY-full-E0001", "amount": 20000,
        }, "pay")
        call(self.s, "payment:wx", "refund_notice", {
            "race_id": "bj2026", "channel": "wx", "notice_id": "R",
            "payment_id": "PAY-full-E0001", "amount": 20000,
        }, "ref")
        race = call(self.s, self.fx.admin, "get_race", {"race_id": "bj2026"}, "g")
        self.assertEqual(race.data["groups"]["full"]["paid"], 0)
        self.assertEqual(race.data["groups"]["full"]["terminated"], 1)


class PersistenceTest(unittest.TestCase):
    def test_state_and_idempotency_survive_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "m.db")
            fx = ServiceFixture(EventStore(path))
            r1 = call(fx.service, "9901", "register", {
                "race_id": "bj2026", "group_id": "full",
                "registrant": registrant("9901"),
            }, "persist-1")
            self.assertTrue(r1.accepted)
            fx.close()

            # 重新打开：历史事件全部回放，状态重建
            store2 = EventStore(path)
            s2 = MarathonEntryService(store2)
            race = call(s2, "admin-1", "get_race", {"race_id": "bj2026"}, "q")
            self.assertEqual(race.data["phase"], "open")
            self.assertEqual(len(race.data["entries"]), 1)
            # 幂等表同样落盘：同一 request_id 返回原结果，不产生重复报名
            r1_again = call(s2, "9901", "register", {
                "race_id": "bj2026", "group_id": "full",
                "registrant": registrant("9901"),
            }, "persist-1")
            self.assertEqual(r1_again.accepted, r1.accepted)
            race2 = call(s2, "admin-1", "get_race", {"race_id": "bj2026"}, "q2")
            self.assertEqual(len(race2.data["entries"]), 1)
            s2.close()


if __name__ == "__main__":
    unittest.main()
