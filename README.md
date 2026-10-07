# local-message-queue-java

基于 Spring Boot 的进程内本地消息队列，用内存状态模拟主题（Topic）、分区（Partition）与消费者组（Consumer Group），
覆盖发布幂等、位点管理、失败重试退避与死信队列，保证重复发布、消费失败与并发消费下的结论一致且可解释。

## 核心组件

| 组件 | 说明 |
| --- | --- |
| `LocalMessageBroker` | 核心实现：主题/分区/消费者组状态、去重索引、在途租约、退避调度、死信队列（重放/统计/保留/清理） |
| `Message` / `DeadLetter` | 消息与死信快照（`attempts` 表示「本次是第几次投递」） |
| `PublishResult` / `CommitResult` / `ReplayResult` | 发布、位点提交与死信重放结果，失败时携带可区分原因 |
| `DeadLetterStats` | 死信统计口径：当前条数 / 累计条数 / 保留上限 |
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

## 死信管理

### 重放规则

- `replayDeadLetter(topic, messageId)` 把仍在死信队列中的消息**重新投回其原分区日志末尾**（新位点、`attempts` 重新计数、消息 ID 保持不变），随后按现有投递与线性退避规则再走一遍。
- 重放**不回退**任何消费者组的已确认位点，也**不阻塞**同分区其它消息：重放消息排在日志末尾，按位点顺序自然投递。
- 重放成功后原死信条目随即移除；若该消息再次重试耗尽，会作为**同一条死信**（同一消息 ID）重新进入死信队列——队列中同一时刻最多只有它的一条条目，累计计数再 +1。
- **重放与去重键的关系**：重放携带非空 `dedupKey` 的死信时，去重索引的归属位点会从「已死信的旧位点」**迁移到重放后的新位点**（旧位点已随死信解决、永不再投递）。因此：
  - 重放之后，同一个分区里同一个去重键仍然**只对应一条有效（可投递）消息**，即重放出来的这条；下游按去重键做幂等不会把它误判为重复，它能被正常消费、确认。
  - 重放之后再用同一去重键发布新消息，结论是确定的：返回 `DEDUPLICATED` 并携带**重放后的那条消息**（新位点、原内容），新消息不写入队列，绝不会指回早已进过死信的旧消息。
  - 若重放后的消息再次耗尽进入死信，索引归属同样停留在最新位点；此后再次重放会继续迁移。不带去重键的消息重放行为不受影响。
- 重放拒绝原因可区分：
  - `UNKNOWN_MESSAGE`：该消息 ID 从未进入过任何主题的死信队列；
  - `TOPIC_MISMATCH`：该消息是其它主题的现存死信，不属于指定主题；
  - `ALREADY_PURGED`：该死信已被主动清理或被保留上限淘汰；
  - `ALREADY_REPLAYED`：该死信已被重放、尚未再次进入死信队列。

### 统计口径

`deadLetterStats(topic)` 与列表查询、按 ID 单查同属一个一致性边界（按主题加锁的死信存储）：

- `currentCount`：当前死信队列中的条数，任意时刻等于 `deadLetters(topic)` 快照的长度；
- `cumulativeCount`：累计进入死信队列的条数，只增不减；重放、清理、淘汰都不会扣减它，同一条消息反复进入会反复计数；
- `maxRetained`：当前保留上限，`null` 表示不限制。

多分区并发产生死信时，列表、单查、`currentCount`、`cumulativeCount` 四处结果收敛后必然一致：列表无重复条目、列表中的每条都能按 ID 查到、条数与统计相符。

### 保留上限与清理

- `setDeadLetterRetention(topic, maxRetained)` 设置每个主题最多保留的死信条数（`>= 1`，传 `null` 取消限制）。
- 超出上限时按**进入死信的先后**淘汰最旧条目（FIFO）；调低上限立即触发淘汰。淘汰视同清理：被淘汰的消息再重放会被拒绝（`ALREADY_PURGED`）。
- 淘汰只发生在死信存储内部，不影响正常分区的位点推进与在途消费，也不影响累计计数。
- `purgeDeadLetter(topic, messageId)` 主动清理已处理的死信：清理后列表与按 ID 查询均不再返回它，其余条目与累计计数不受影响。

### 兼容范围

- 发布幂等、位点管理、乱序确认、重试退避与既有死信查询的 API 形态均未改变；本次仅修正「携带去重键的死信被重放」这一场景下的去重索引归属（由不迁移改为随重放迁移）。不带去重键的消息重放、重放后再次失败重新进入死信、位点不回退、不阻塞同分区其它消息等既有行为保持不变。
- 死信存储为进程内内存状态，重启即丢失；墓碑（已重放/已清理的消息 ID）同样不持久化。

## 并发一致性边界

- 所有状态变更在「分区锁」下串行化：同一分区的发布/拉取/确认/失败处理互斥，不同分区互不阻塞。
- 死信存储按主题独立加锁（锁顺序：分区锁 → 死信存储锁），多分区并发产生死信不会互相污染列表/单查/统计视图。
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

# 死信管理：统计 / 重放 / 保留上限 / 清理
curl 'localhost:8080/api/queue/dead-letters/stats?topic=orders'
curl -X POST localhost:8080/api/queue/dead-letters/replay    -H 'Content-Type: application/json' -d '{"topic":"orders","messageId":"<死信消息ID>"}'
curl -X POST localhost:8080/api/queue/dead-letters/retention -H 'Content-Type: application/json' -d '{"topic":"orders","maxRetained":100}'
curl -X POST localhost:8080/api/queue/dead-letters/purge     -H 'Content-Type: application/json' -d '{"topic":"orders","messageId":"<死信消息ID>"}'
```

手工验证重放闭环的典型路径：发布一条消息 → 反复 `nack` 使其进入死信 → 查 `dead-letters/stats` 确认
`currentCount=1, cumulativeCount=1` → `replay` 重放 → 再次 `poll` 能在原分区日志末尾拉到它（`attempts` 从 1
重新计数）→ `commit` 确认，已确认位点继续前进不回退。

携带去重键消息的重放语义可按下面路径复现（也是自动化用例
`replayMigratesDedupKeySoReplayedMessageIsConsumableAndRepublishIsDeterministic` 走完的流程）：

1. 用带 `dedupKey` 的消息发布（`offset=0`），重复发布同一键得到 `DEDUPLICATED` 且指回 `offset=0`；
2. 反复 `nack` 使其重试耗尽进入死信，确认 `committed=1` 已推进、死信列表 1 条；
3. `replay` 重放：成功，消息以**新位点 `offset=1`** 投回原分区日志末尾，`committed` 不回退；
4. `poll` 下游按去重键做幂等：同一分区该键只拉到这一条有效消息，且它从未被成功处理过，正常消费并 `commit(offset=1)`，`committed=2`；
5. 再用同一 `dedupKey` 发布新内容：返回 `DEDUPLICATED`，携带的是**重放后的消息**（`offset=1`、原内容），`logEndOffset` 保持 2，新消息不入队；
6. 再次 `replay` 同一消息 ID：被拒绝，原因 `ALREADY_REPLAYED`。

```bash
# 只跑死信管理测试，或只跑上面那条复现用例（日志含消息键、去重键、位点与判定依据）
mvn test -Dtest=DeadLetterManagementTests
mvn test -Dtest=DeadLetterManagementTests#replayMigratesDedupKeySoReplayedMessageIsConsumableAndRepublishIsDeterministic
```

## 测试覆盖

`LocalMessageBrokerTests` 覆盖：

1. 分区内顺序投递、乱序确认与重放拒绝（位点不回退）；
2. 四类位点提交拒绝原因可区分；
3. 去重键重复发布仅首次生效（含跨分区独立性）；
4. 失败重试线性退避、重试耗尽进入死信、死信可单独查询、死信不阻塞位点推进；
5. 32 线程并发发布同一去重键恰好生效一次；
6. 8 线程并发消费 200 条消息（含毒消息）：位点、去重、死信状态最终一致，同一条消息不被重复成功消费。

`DeadLetterManagementTests` 覆盖：

1. 死信重放：投回原分区日志末尾、位点不回退、不阻塞同分区后续消息、重放后可正常消费确认、重复重放拒绝；
2. 重放后再次失败：同一消息 ID 在死信队列中仍只有一条，累计计数 +1；
3. **带去重键发布 → 重试耗尽进死信 → 重放 → 再消费 / 再发布**：去重索引归属迁移到新位点，重放消息是同分区同键唯一可投递消息且能被下游正常消费，再用同键发布确定性地 `DEDUPLICATED` 指回重放消息；
4. **重放后再次失败再发布**：去重口径跟随最近一次重放停留在最新位点，再次重放后同键仍只有一条可投递消息，位点不回退；
5. 重放拒绝原因可区分：`UNKNOWN_MESSAGE` / `TOPIC_MISMATCH` / `ALREADY_PURGED` / `ALREADY_REPLAYED`；
6. 4 分区 × 50 条毒消息并发进入死信（另有并发读线程）：列表 / 单查 / 当前条数 / 累计条数四处一致、无重复条目、查询不报错；
7. 保留上限：超限按进入先后淘汰最旧、调低上限立即淘汰、淘汰视同清理、主动清理不影响累计计数、淘汰不影响分区位点。
