# 赛事报名管理

面向马拉松名额争议场景的报名后端：统一管理赛事、组别、报名人、支付与退款，
让**分批放号**与**候补递补**遵循确定顺序，支付/退款通知重送只作用于原报名，
退赛在**退款完成后**才把名额交给下一位候补。

## 目录

| 路径 | 职责 |
| --- | --- |
| `app/contracts.py` | `Request`/`Result` 约定与领域错误 |
| `app/actions.py` | 全部动作与状态常量 |
| `app/store.py` | SQLite 表结构、写事务、只增事件日志 |
| `app/service.py` | 领域服务：状态机、放号、递补、支付退款、授权 |
| `app/api.py` | 本地 JSON 调用入口（单请求或批处理） |
| `tests/test_service.py` | 16 个行为测试，覆盖全部争议场景 |

## 核心规则

1. **同证不重占**：同一证件（类型 + 证件号）在同一赛事只允许一条有效报名；
   部分唯一索引兜底，并发重复提交也只有一个赢家。
2. **幂等双保险**：调用方 `request_id` 保证命令重放返回首次结果；
   支付/退款另以网关 `notification_id` 去重，重送只返回原报名状态，
   不二次入账、不二次释放名额。
3. **确定顺序**：名额是带 `seq` 的具身份实体，放号按批次连续编号；
   报名占用最小 `seq` 的空闲名额；候补按 `waitlist_seq` FIFO，每次释放只递补一人。
4. **退赛闸门**：未付款退赛立即释放；已付款只进入 `refund_pending`，
   必须等 `refund.notify` 到达才置 `refunded`、释放名额并触发唯一一次递补。
5. **状态不倒退**：报名截止后拒绝新报名与放号（在册候补仍可递补）；
   抽签公布后候补落 `lot_out` 终态，终态记录不可再变，释放名额不再流向落选者。
6. **分组授权**：赛事创建者拥有全部权限；工作人员须经 `category.grant`
   授权才能对该组别放号或取消报名，越权返回 `forbidden`。
7. **事件可还原**：所有变更先写只增 `events` 表（`seq` 单调），
   `slot.timeline` 可还原任意名额 *发放 → 占用 → 付款 → 释放/退款 → 递补* 的全过程。

## 状态机

```
报名: waiting ──(递补)──► held ──付款──► paid ──退赛──► refund_pending ──退款完成──► refunded
                            │                                          （释放并递补）
                            └──未付款退赛/取消──► withdrawn | cancelled（释放并递补）
抽签: waiting ──公布──► lot_out（终态）

名额: free ──占用──► held ──付款──► confirmed ──释放──► free ──递补──► held …
```

## 测试

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall app
```

## 使用

数据默认落在 `:memory:`，设置 `MARATHON_DB` 可持久化：

```bash
MARATHON_DB=marathon.db python3 -m app.api <<'JSON'
{"requests": [
  {"actor": "org", "action": "event.create", "request_id": "r1",
   "payload": {"race_id": "R1", "name": "北京马拉松"}},
  {"actor": "org", "action": "category.create", "request_id": "r2",
   "payload": {"category_id": "C1", "race_id": "R1", "name": "全程马拉松",
               "fee_fen": 20000, "quota": 30000}},
  {"actor": "org", "action": "category.open_batch", "request_id": "r3",
   "payload": {"category_id": "C1", "count": 5000}},
  {"actor": "110101", "action": "registrant.register", "request_id": "r4",
   "payload": {"category_id": "C1", "id_type": "idcard", "id_no": "110101",
               "person_name": "张三", "registration_id": "REG-1"}},
  {"actor": "pay", "action": "payment.notify", "request_id": "r5",
   "payload": {"registration_id": "REG-1", "notification_id": "PAY-1001"}},
  {"actor": "auditor", "action": "slot.timeline",
   "payload": {"slot_id": "<上一步返回的 slot_id>"}}
]}
JSON
```

退出码：全部受理 `0`；存在业务拒绝 `1`；请求格式非法 `2`。
