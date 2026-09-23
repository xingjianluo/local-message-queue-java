# local-message-queue-java

基于 Spring Boot 的进程内本地消息队列，用内存状态模拟主题（Topic）、分区（Partition）与消费者组（Consumer Group），
覆盖发布幂等、位点管理、失败重试退避与死信队列，保证重复发布、消费失败与并发消费下的结论一致且可解释。

## 核心组件

| 组件 | 说明 |
| --- | --- |
| `LocalMessageBroker` | 核心实现：主题/分区/消费者组状态、去重索引、在途租约、退避调度、死信队列 |
| `Message` / `DeadLetter` | 消息与死信快照（`attempts` 表示「本次是第几次投递」） |
| `PublishResult` / `CommitResult` | 发布与位点提交结果，失败时携带可区分原因 |
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
```

## 测试覆盖

`LocalMessageBrokerTests` 覆盖：

1. 分区内顺序投递、乱序确认与重放拒绝（位点不回退）；
2. 四类位点提交拒绝原因可区分；
3. 去重键重复发布仅首次生效（含跨分区独立性）；
4. 失败重试线性退避、重试耗尽进入死信、死信可单独查询、死信不阻塞位点推进；
5. 32 线程并发发布同一去重键恰好生效一次；
6. 8 线程并发消费 200 条消息（含毒消息）：位点、去重、死信状态最终一致，同一条消息不被重复成功消费。
