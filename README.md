# 北京马拉松报名后端

一套基于**事件溯源（Event Sourcing）**的赛事报名后端，统一管理赛事、组别、报名人、
支付与退款。系统只追加事件、从不就地修改状态，任何报名/名额/资金争议都可以按事件
序列完整还原。

## 针对报名当天事故的保证

| 事故 | 系统的确定性保证 |
| --- | --- |
| 同一证件重复提交占住多个名额 | 全赛事证件唯一索引；同一 `client_token` 合并到原报名，换新令牌同样拒绝；并发写入由流版本号 + 事务仲裁，8 个并发请求只有 1 个成功 |
| 一笔退款后已释放席位被再次错误占用 | 名额释放与下一位候补占用在**同一事务**内完成；退款通知按 `(channel, notice_id)` 去重，重放零副作用；换通知号再退同一笔支付单被拒（状态 `refunded`）；报名终止后的迟到付款被拒（状态 `terminated`） |
| 人工名单不可信 | 唯一事实来源是只追加事件表；当前状态随时可由事件流重放重建，工作人员名单不再被信任 |
| 分批放号/候补递补顺序不确定 | 候补顺序确定：先到先得组按报名事件序号，抽签组按抽签排名；名额一释放立即在同事务内交给队首，空名额与候补队列永不同时非空 |
| 状态被人为倒退 | 赛事阶段 `setup → open → closed → published` 单向；截止后不能重开、不能放号；抽签结果不可重抽，公布后不可再抽签 |
| 工作人员越权操作别组 | 工作人员按组别授权（`*` 为全组别）；放号、完成退赛、查询均做组别范围校验 |
| 争议无法举证 | `slot_history` / `entry_history` 按全局序号还原名額「发放→占用→付款→释放/退款→递补」的全部事件，每条带事件 ID、操作人、时间 |

## 架构

```
contracts.py  请求/结果约定（request_id 幂等键）
store.py      SQLite 只追加事件存储（events / requests / notices 三表，单事务提交）
domain.py     纯函数核心：fold 事件折叠投影 + decide 命令决策（全部业务规则与守卫）
service.py    应用编排：幂等、通知去重、鉴权、查询与争议还原
api.py        标准输入/输出 JSON 命令入口
```

写入路径：`读事件流 → fold 当前状态 → decide 决策 → 单事务追加事件 + 通知去重 + 命令响应`。
领域校验不通过时不产生任何事件。

### 名额生命周期

```
QuotaIssued(发放) → SlotHeld(占用) → PaymentRecorded(付款)
   → PaymentRefunded(退款) / complete_withdrawal(退赛完成)
   → SlotFreed(释放) → EntryTerminated(终止) → SlotHeld(下一位候补立即递补)
```

退赛**申请**（`request_withdrawal`）不释放名额；只有退款完成或工作人员完成退赛，
名额才释放并立即递补。候补者退赛直接终止出队，从不触碰名额。

### 赛事阶段

`setup`（建赛/设组/授权）→ `open`（报名、分批放号）→ `closed`（截止、抽签）
→ `published`（结果公布，中签者占名额）。阶段只进不退。

## 命令清单

写入类（均需 `race_id`，返回中 `data.events` 列出本次产生的全部事件）：

| 动作 | 操作人 | 说明 |
| --- | --- | --- |
| `create_race` | 管理员 | 创建赛事，创建者即管理员 |
| `add_group` | 管理员 | 添加组别（capacity 容量、fee 分、mode=`fcfs`/`lottery`） |
| `authorize_staff` | 管理员 | 授权工作人员到指定组别（`["*"]` 为全部） |
| `open_registration` / `close_registration` | 管理员 | 开放 / 截止报名（单向） |
| `register` | 报名人本人（actor=证件号） | 报名；带 `client_token` 防连点 |
| `release_quota` | 该组别授权人员 | 分批放号（`batch_id` 不可重复，累计不超容量），自动递补 |
| `run_lottery` / `publish_lottery` | 管理员 | 抽签（可复现种子）/ 公布并按排名占用 |
| `payment_notice` | `payment:<channel>` | 支付通知；金额须等于组别费用，重复通知去重 |
| `refund_notice` | `payment:<channel>` | 退款通知；完成即释放名额并递补 |
| `request_withdrawal` | 报名人/授权人员 | 申请退赛（占名额者仅登记，候补者直接终止） |
| `complete_withdrawal` | 该组别授权人员 | 完成退赛：此刻才释放名额并递补，已付款挂 `refund_due` |

查询类：`get_race`（管理员/授权工作人员，按授权裁剪组别）、`get_waitlist`、
`get_entry`、`slot_history`（争议还原名额时间线）、`entry_history`（报名时间线）。

## 使用

数据默认保存在 `MARATHON_DB` 指定的 SQLite 文件（默认 `marathon.db`）。

```bash
echo '{"actor":"admin","action":"create_race",
       "payload":{"race_id":"bj2026","name":"北京马拉松"},
       "request_id":"r-1"}' | python3 -m app.api
```

支付/退款通知示例（`actor` 必须与通道一致，防止伪造）：

```json
{
  "actor": "payment:wechat", "action": "payment_notice",
  "payload": {"race_id": "bj2026", "channel": "wechat", "notice_id": "W-2026-0001",
              "payment_id": "PAY-full-E0001", "amount": 20000},
  "request_id": "pay-0001"
}
```

争议还原：

```bash
echo '{"actor":"admin","action":"slot_history",
       "payload":{"race_id":"bj2026","slot_id":"full-S0001"},
       "request_id":"h-1"}' | python3 -m app.api
```

## 测试

```bash
python3 -m unittest discover -s tests   # 29 个行为测试
python3 -m compileall app               # 构建检查
```

覆盖：同证件并发重复报名、提交令牌合并、分批放号与确定顺序递补、空名额直接占用、
支付/退款通知重放、退款释放即递补、迟到付款拒绝、退赛申请与完成的区别、
阶段不可逆、抽签确定性与公布后守卫、组别授权、名额/报名时间线还原、SQLite 重启后
状态重建与幂等保持。
