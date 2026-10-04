# local-message-queue-java

基于 Spring Boot 的进程内本地消息队列，用内存状态模拟主题（Topic）、分区（Partition）与消费者组（Consumer Group），
覆盖发布幂等、位点管理、失败重试退避与死信队列，保证重复发布、消费失败与并发消费下的结论一致且可解释。

## 核心组件

| 组件 | 说明 |
| --- | --- |
| `LocalMessageBroker` | 核心实现：主题/分区/消费者组状态、去重索引、在途租约、退避调度、死信队列 |
| `Message` / `DeadLetter` | 消息与死信快照（`attempts` 表示「本次是第几次投递」） |
| `PublishResult` / `CommitResult` / `ReplayResult` | 发布、位点提交与死信重放结果，失败时携带可区分原因 |
| `DeadLetterStats` | 死信统计快照：当前条数 / 累计进入条数 / 保留上限 |
| `QueueController` | REST 入口，便于手工验证 |

## 投递与确认语义

- 位点 `offset` 从 0 起单调递增；`committed` 表示「下一个待确认位点」，只会前进不会回退。
- 同一分区内消息严格按发布顺序投递；已投递未确认的消息持有「在途租约」，任何并发 `poll` 都不会重复投递它。
- `commit(offset)` 支持乱序确认：已确认位点沿「连续已解决位点」自动推进；单条消息失败不会阻塞同分区其他位点推进。
- 位点提交拒绝原因可区分：
  - `OFFSET_REGRESSION`：提交位点小于已确认位点（重复消费/重放），已确认位点不回退；
  - `OFFSET_OUT_OF_RANGE`：提交位点超过分区最大位点；
  - `UNKNOWN_GROUP`：消费者组未订阅该主题，位点不属于该组；
  - `UNKNOWN_PARTITION`：主题或分区不存在。

## 去重与重试规则

- 非空 `dedupKey` 在「主题 + 分区」维度幂等：并发发布同一去重键只有首次 `ACCEPTED`，其余返回 `DEDUPLICATED` 及首次消息。
- `nack` 表示消费失败：消息进入线性退避，延迟 = `backoffMillis × (已失败次数)`，退避期内不会被投递。
- 投递次数达到 `maxAttempts`（默认 3）后消息进入死信队列；死信位点视为已解决，已确认位点照常推进。
- 死信可按主题列表查询，也可按消息 ID 单独查询。

## 死信重放规则

- `replayDeadLetter(topic, messageId)` 把死信以「同一消息 ID、新位点」追加到**原分区**日志末尾，
  重新走现有的投递 / 退避 / 重试流程；重放副本的 `attempts` 从 0 重新计数。
- 重放不触碰任何消费者组的已确认位点（位点不回退），也不影响同分区其他在途消息的消费。
- 重放成功后该消息立即离开死信队列；若再次重试耗尽会重新进入死信，
  **同一消息 ID 在死信中至多一条**（新条目替换旧条目，`totalEntered` 再计一次）。
- 重放副本的去重键置空：重放不是新发布，不占用「主题+分区」去重索引，原有发布幂等语义不变。
- 重放拒绝原因可区分：
  - `DEAD_LETTER_NOT_FOUND`：该消息当前不在死信队列（从未进入过）；
  - `DEAD_LETTER_PURGED`：曾经进入死信，但已被主动清理或被淘汰（留有墓碑）；
  - `TOPIC_MISMATCH`：死信存在，但不属于指定主题。

## 死信统计口径

- `deadLetterStats(topic)` 返回统一口径：
  - `current`：当前死信条数，与 `deadLetters(topic)` 列表长度、`deadLetter(messageId)` 可查到的事实严格一致；
  - `totalEntered`：累计进入死信的条数，每进入一次计一次，清理 / 淘汰 / 重放都不回减；
  - `retentionLimit`：当前保留上限，`-1` 表示不限。
- 所有死信结构（列表、ID 索引、计数、墓碑）的读写都在同一把「死信锁」下串行化：
  多分区并发产生死信时，列表、单查、统计三处结果相互对得上，同一条消息不会重复出现，
  同一条消息「在不在死信里」的结论不会反复。

## 死信保留与清理

- `setDeadLetterRetention(topic, maxEntries)` 设置单主题死信保留上限（`< 0` 表示不限，默认不限）。
- 超出上限时按「进入死信的先后」淘汰最旧条目（列表头先出）；设置 / 调低上限立即对存量生效。
- 淘汰只影响死信视图：正常分区的位点推进与在途消费不受任何影响。
- `purgeDeadLetter(topic, messageId)` 主动清理一条已处理的死信：清理后列表与单查都不再返回它，
  其余死信与 `totalEntered` 累计计数不受影响；重复清理或跨主题清理返回 `false`。
- 被淘汰 / 被清理的消息 ID 留下墓碑，后续重放会得到 `DEAD_LETTER_PURGED`，
  与「从未进入死信」的 `DEAD_LETTER_NOT_FOUND` 可区分。

## 兼容范围

- 本轮只新增死信运维能力（重放 / 统计 / 保留 / 清理），发布幂等、位点管理、退避重试、
  死信产生与查询的既有语义一律不变，既有用例全部保持通过。
- 重放产生的新位点参与既有乱序确认与位点推进规则；墓碑仅用于重放拒绝原因的区分，不影响任何投递路径。

## 并发一致性边界

- 所有状态变更在「分区锁」下串行化：同一分区的发布/拉取/确认/失败处理互斥，不同分区互不阻塞。
- 并发发布同一去重键：恰好一次写入日志，其余得到同一条首次消息。
- 并发消费：同一条消息同一时刻只会被一个消费者拿到；非毒消息恰好成功消费一次；毒消息重试耗尽后恰好进入死信一次。
- 已确认位点单调不减；任何重放、越界或跨组提交都会被拒绝且不影响现有位点。

## 本地验证

```bash
# 运行全部单测（日志打印消息键、位点与判定依据）
mvn test

# 启动服务后手工验证
mvn spring-boot:run

curl -X POST localhost:8080/api/queue/topics        -H 'Content-Type: application/json' -d '{"topic":"orders","partitions":2}'
curl -X POST localhost:8080/api/queue/subscriptions -H 'Content-Type: application/json' -d '{"group":"g1","topic":"orders"}'
curl -X POST localhost:8080/api/queue/publish       -H 'Content-Type: application/json' -d '{"topic":"orders","partition":0,"key":"k1","dedupKey":"d1","payload":"hello"}'
curl 'localhost:8080/api/queue/poll?group=g1&topic=orders&partition=0&max=10'
curl -X POST localhost:8080/api/queue/commit        -H 'Content-Type: application/json' -d '{"group":"g1","topic":"orders","partition":0,"offset":0}'
curl -X POST localhost:8080/api/queue/nack          -H 'Content-Type: application/json' -d '{"group":"g1","topic":"orders","partition":0,"offset":0}'
curl 'localhost:8080/api/queue/offsets?group=g1&topic=orders&partition=0'
curl 'localhost:8080/api/queue/dead-letters?topic=orders'

# 死信运维：重放 / 统计 / 保留上限 / 主动清理
curl -X POST localhost:8080/api/queue/dead-letters/replay    -H 'Content-Type: application/json' -d '{"topic":"orders","messageId":"<消息ID>"}'
curl 'localhost:8080/api/queue/dead-letters/stats?topic=orders'
curl -X POST localhost:8080/api/queue/dead-letters/retention -H 'Content-Type: application/json' -d '{"topic":"orders","maxEntries":100}'
curl -X POST localhost:8080/api/queue/dead-letters/purge     -H 'Content-Type: application/json' -d '{"topic":"orders","messageId":"<消息ID>"}'
```

## 测试覆盖

`LocalMessageBrokerTests` 覆盖：

1. 分区内顺序投递、乱序确认与重放拒绝（位点不回退）；
2. 四类位点提交拒绝原因可区分；
3. 去重键重复发布仅首次生效（含跨分区独立性）；
4. 失败重试线性退避、重试耗尽进入死信、死信可单独查询、死信不阻塞位点推进；
5. 32 线程并发发布同一去重键恰好生效一次；
6. 8 线程并发消费 200 条消息（含毒消息）：位点、去重、死信状态最终一致，同一条消息不被重复成功消费。

`DeadLetterOpsTests` 覆盖：

1. 死信重放：投回原分区新位点、消息 ID 不变、可正常消费提交、位点不回退、统计口径一致；
2. 重放后再次耗尽：同一消息 ID 在死信中仍只有一条，`totalEntered` 累计 +1；
3. 重放拒绝原因可区分：`DEAD_LETTER_NOT_FOUND` / `DEAD_LETTER_PURGED` / `TOPIC_MISMATCH`；
4. 4 分区并发产生死信：读线程持续校验「列表 / 单查 / 统计」三处口径一致、无重复条目；
5. 保留上限按进入先后淘汰最旧、调低上限立即生效、淘汰不影响位点推进；
   主动清理后查询不再返回、累计计数不变、已清理消息重放给出 `DEAD_LETTER_PURGED`。
