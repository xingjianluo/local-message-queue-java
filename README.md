# local-message-queue-java-bootstrap-a

进程内本地消息队列：用 JVM 内存状态模拟 **主题（Topic）/ 分区（Partition）/ 消费者组（Consumer Group）**，
支持幂等发布、顺序投递、位点管理、失败重试退避与死信队列。所有状态保存在内存中，进程重启即丢失，
仅用于语义演示与本地验证，不依赖任何外部中间件。

## 1. 核心模型

| 概念 | 实现 |
| --- | --- |
| Topic | `createTopic(topic, partitions)` 建立，分区数固定 |
| Partition | 每个分区一条只追加日志（`List<Message>`），位点 `offset` 从 0 起连续递增 |
| Message | `messageId / topic / partition / offset / key / dedupKey / payload / timestamp`，不可变 |
| Consumer Group | `registerGroup` 订阅主题；每个组在每个分区上独立维护已确认位点与投递裁决状态 |

每条消息在一个组的一个分区上有生命周期状态：

```
READY -> IN_FLIGHT -> CONFIRMED        （commit，正常终结）
                     \-> RETRY_WAIT -> IN_FLIGHT ...（退避到期后重投）
                                       \-> DEAD（重试耗尽，进入死信队列，终结）
```

## 2. 投递与确认语义

- **分区内顺序稳定**：消息按发布顺序获得连续位点；`poll` 从已确认水位之后按位点升序返回。
- **位点语义**：已确认位点 `committed` 表示 `0..committed` 的消息全部终结（CONFIRMED 或 DEAD）。
  水位只增不减，由连续终结前缀自动推进；允许先确认后面的消息，但在“空洞”（未终结的前序消息）
  补齐前水位停在空洞之前 —— 因此**单条消息失败只阻塞它自己的位点，不阻塞同分区其他消息的处理与确认**。
- **不重复投递**：`poll` 跳过 IN_FLIGHT（已投递未裁决）、退避未到期的 RETRY_WAIT，以及
  CONFIRMED/DEAD 终结消息；重投时返回的是**同一条消息**（messageId / offset 不变），不会产生新消息。
- **提交裁决**（拒绝原因可区分）：
  - `UNKNOWN_GROUP`：该消费者组不存在或未订阅该主题；
  - `OFFSET_OUT_OF_RANGE`：位点为负或不小于分区日志长度；
  - `OFFSET_REGRESSION`：提交位点小于已确认水位（重放不得使已确认位点回退）；
  - 提交位点等于当前水位（重放已确认位点）**幂等放行**，状态不变。
- **组隔离**：不同消费者组的位点、重试、死信状态完全独立；向 A 组提交不会影响 B 组。

## 3. 去重与重试规则

### 幂等发布

- `publish` 可带 `dedupKey`。同一 **topic** 下相同 dedupKey 的发布只有**首次生效**，
  返回 `PUBLISHED` 与新位点；后续返回 `DUPLICATE` 并带回**首次那条消息**（同 messageId、同 offset），
  不产生新日志、不新增位点。
- dedupKey 为 null 时不去重。去重判定与日志追加在同一临界区内完成，并发发布下结论一致。

### 重试与退避

- 消费失败调用 `nack(group, topic, partition, offset, error)`：
  - 处理次数 `attempts` 未超过 `localmq.max-retries`（默认 3）：进入 RETRY_WAIT，
    指数退避 `base * 2^(attempts-1)`，base 由 `localmq.backoff-base-millis`（默认 100ms）配置；
  - `attempts > maxRetries`：进入 DEAD，写入该死信组的死信队列（`DeadLetter` 含消息、组、
    尝试次数、最后错误、入队时间），可通过 `deadLetters(group)` 或 `findDeadLetter(group, messageId)` 单独查询。
- DEAD 与 CONFIRMED 同为终结态，参与水位推进；死信消息不再投递。

## 4. 并发一致性边界

- 全局一把可重入锁保护所有状态（topic 日志、去重索引、各组位点与裁决）。
  发布、poll、commit、nack、水位推进均为原子临界区，因此：
  - 并发发布同 dedupKey：恰好 1 次 PUBLISHED，其余全为 DUPLICATE，且指向同一 messageId；
  - 并发消费：一条消息同一时刻至多被一个消费者 IN_FLIGHT 投递，不存在并发重复投递；
    成功消息恰好处理 1 次，失败消息严格串行重投；
  - 位点单调不减，死信集合与重试次数确定、可解释。
- 临界区内不执行用户回调（测试/调用方在锁外执行业务逻辑），避免用户代码持锁。
- **边界**：状态仅在单 JVM 内存有效，不跨进程、不持久化；若消费者拉取后既不 commit 也不 nack
  （宕机模型），消息停留 IN_FLIGHT，当前版本不做自动重平衡/超时回收。

## 5. HTTP 接口（演示用）

```
POST /mq/topics/{topic}?partitions=n
POST /mq/topics/{topic}/groups/{group}
POST /mq/topics/{topic}/partitions/{partition}/publish     body: {"key":"...","dedupKey":"...","payload":"..."}
GET  /mq/groups/{group}/topics/{topic}/partitions/{partition}/poll?max=10
POST /mq/groups/{group}/topics/{topic}/partitions/{partition}/commit?offset=0
POST /mq/groups/{group}/topics/{topic}/partitions/{partition}/nack?offset=0&error=boom
GET  /mq/groups/{group}/committed?topic={topic}&partition={partition}
GET  /mq/groups/{group}/dead-letters
```

## 6. 本地验证

```bash
# 运行全部单测（4 个场景，日志中含 key / offset / 判定依据）
mvn test

# 只跑消息队列测试
mvn test -Dtest=LocalMessageQueueTests

# 启动服务后手工验证
mvn spring-boot:run
curl -X POST 'localhost:8080/mq/topics/orders?partitions=2'
curl -X POST 'localhost:8080/mq/topics/orders/groups/order-workers'
curl -X POST localhost:8080/mq/topics/orders/partitions/0/publish \
  -H 'Content-Type: application/json' \
  -d '{"key":"k1","dedupKey":"d-1","payload":"hello"}'
# 重复发布同样的 dedupKey，第二次返回 "DUPLICATE" 且 offset 不变
curl 'localhost:8080/mq/groups/order-workers/topics/orders/partitions/0/poll'
curl -X POST 'localhost:8080/mq/groups/order-workers/topics/orders/partitions/0/commit?offset=0'
```

测试报告位于 `target/surefire-reports/`，可搜索「判定」查看每条发布/投递/提交/死信的
消息键、位点与判定依据。

### 测试场景清单（`LocalMessageQueueTests`）

1. `commitReasonsAreDistinguishableAndOffsetsNeverRegress`：越界 / 未注册组 / 回退三类拒绝原因可区分，
   相等位点重放幂等，水位不回退。
2. `duplicatePublishByDedupKeyOnlyFirstTakesEffectUnderConcurrency`：16 线程并发同 dedupKey 发布，
   恰好 1 次 PUBLISHED。
3. `failedMessageRetriesWithBackoffAndEntersDeadLetterWithoutBlockingOthers`：退避期不投递、到期重投同一条消息、
   重试耗尽入死信、死信消息不阻塞后续位点推进。
4. `concurrentConsumersKeepOffsetsDedupAndDeadLettersConsistent`：8 消费者并发消费 2 分区，
   校验最终位点到底、死信数量确定、成功消息投递 1 次而毒消息严格串行重投、消费者组间隔离。
