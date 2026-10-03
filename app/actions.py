"""全部服务动作与状态常量，集中管理避免魔法字符串。"""

# ---- 赛事 ----
EVENT_CREATE = "event.create"
EVENT_PUBLISH_LOTTERY = "event.publish_lottery"   # 公布抽签结果（终态化所有报名）
EVENT_CLOSE_REGISTRATION = "event.close_registration"

# ---- 组别 ----
CATEGORY_CREATE = "category.create"
CATEGORY_OPEN_BATCH = "category.open_batch"       # 分批放号
CATEGORY_GRANT = "category.grant"                 # 授权工作人员
CATEGORY_REVOKE = "category.revoke"

# ---- 报名人 ----
REGISTRANT_REGISTER = "registrant.register"       # 报名（占用下一个可用名额或进候补）
REGISTRANT_WITHDRAW = "registrant.withdraw"       # 报名人退赛

# ---- 工作人员 ----
STAFF_CANCEL = "staff.cancel"                     # 工作人员取消报名（需授权）

# ---- 支付网关通知 ----
PAYMENT_NOTIFY = "payment.notify"                 # 支付成功通知（按通知流水号幂等）
REFUND_NOTIFY = "refund.notify"                   # 退款完成通知（退赛完成 -> 名额才递补）

# ---- 查询（不走幂等表）----
SLOT_TIMELINE = "slot.timeline"                   # 还原指定名额全部事件
CATEGORY_LIST = "category.list"
REGISTRANT_LOOKUP = "registrant.lookup"

# ---- 报名记录状态（只进不退）----
# waiting    候补排队中
# held       已占用名额、待支付
# paid       已付款、名额确认
# refund_pending 已退赛/取消，等待退款完成
# refunded   退款完成（终态）
# withdrawn  未支付直接退赛释放（终态）
# cancelled  工作人员取消（未支付，终态）
# lot_out    抽签未中（终态）
REGISTRATION_STATES = {
    "waiting",
    "held",
    "paid",
    "refund_pending",
    "refunded",
    "withdrawn",
    "cancelled",
    "lot_out",
}

# 终态：任何情况下都不能再改变
TERMINAL_STATES = {"refunded", "withdrawn", "cancelled", "lot_out"}

# ---- 组别生命周期 ----
# registrations_open -> closed -> lottery_published
CAT_OPEN = "registrations_open"
CAT_CLOSED = "closed"
CAT_LOTTERY = "lottery_published"

# ---- 名额状态 ----
SLOT_FREE = "free"
SLOT_HELD = "held"
SLOT_CONFIRMED = "confirmed"
