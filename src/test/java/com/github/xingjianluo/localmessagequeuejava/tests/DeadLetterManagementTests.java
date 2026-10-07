package com.github.xingjianluo.localmessagequeuejava.tests;

import com.github.xingjianluo.localmessagequeuejava.broker.DeadLetter;
import com.github.xingjianluo.localmessagequeuejava.broker.DeadLetterStats;
import com.github.xingjianluo.localmessagequeuejava.broker.LocalMessageBroker;
import com.github.xingjianluo.localmessagequeuejava.broker.Message;
import com.github.xingjianluo.localmessagequeuejava.broker.PublishOutcome;
import com.github.xingjianluo.localmessagequeuejava.broker.PublishResult;
import com.github.xingjianluo.localmessagequeuejava.broker.ReplayRejectionReason;
import com.github.xingjianluo.localmessagequeuejava.broker.ReplayResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 死信管理测试：重放、多分区并发死信视图一致性、保留上限与主动清理。
 * 关键判定均打印「消息键 / 位点 / 判定依据」。
 */
public class DeadLetterManagementTests {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterManagementTests.class);

    /**
     * 让指定消息重试耗尽进入死信（backoff=0）。
     * 前置条件：消息当前处于在途状态（刚被 poll 投递）；每次 nack 后拉取下一次重投。
     */
    private static Message driveToDeadLetter(LocalMessageBroker broker, String group, String topic,
                                             int partition, long offset, int maxAttempts) {
        Message last = null;
        for (int delivered = 1; delivered < maxAttempts; delivered++) {
            last = broker.nack(group, topic, partition, offset);
            if (delivered < maxAttempts - 1) {
                List<Message> redelivered = broker.poll(group, topic, partition, 64).stream()
                        .filter(m -> m.offset() == offset).toList();
                assertFalse(redelivered.isEmpty(), "第 " + (delivered + 1) + " 次投递应能拿到 offset=" + offset);
            }
        }
        return last;
    }

    @Test
    void replayRequeuesToOriginalPartitionWithoutRegressingOffsets() {
        LocalMessageBroker broker = new LocalMessageBroker(3, 0L, Clock.systemUTC());
        broker.createTopic("orders-dlq", 1);
        broker.subscribe("g-ops", "orders-dlq");
        broker.publish("orders-dlq", 0, "k-good", null, "ok");
        Message poison = broker.publish("orders-dlq", 0, "k-poison", null, "bad").message();

        broker.poll("g-ops", "orders-dlq", 0, 10);
        broker.commit("g-ops", "orders-dlq", 0, 0);
        Message dead = driveToDeadLetter(broker, "g-ops", "orders-dlq", 0, 1, 3);
        assertEquals(3, dead.attempts());
        assertEquals(2L, broker.committedOffset("g-ops", "orders-dlq", 0));
        assertEquals(1, broker.deadLetters("orders-dlq").size());
        log.info("重试耗尽 key=k-poison offset=1 attempts=3 判定=进入死信, committed=2 已推进");

        // 重放：投回原分区日志末尾，位点不回退
        ReplayResult replayed = broker.replayDeadLetter("orders-dlq", poison.id());
        assertTrue(replayed.success());
        assertEquals(poison.id(), replayed.message().id());
        assertEquals(2L, replayed.message().offset());
        assertEquals(0, replayed.message().attempts());
        assertEquals(3L, broker.logEndOffset("orders-dlq", 0));
        assertEquals(2L, broker.committedOffset("g-ops", "orders-dlq", 0));
        log.info("重放 key=k-poison id={} 新offset=2 判定=投回原分区日志末尾, committed 保持 2 不回退", poison.id());

        // 重放后死信队列清空，但累计计数保留
        assertTrue(broker.deadLetters("orders-dlq").isEmpty());
        DeadLetterStats stats = broker.deadLetterStats("orders-dlq");
        assertEquals(0, stats.currentCount());
        assertEquals(1, stats.cumulativeCount());
        log.info("重放后 判定=死信列表清空, currentCount=0 cumulativeCount=1（累计只增不减）");

        // 重放不阻塞同分区其它消息：新消息追加在重放消息之后，按序一起投递
        broker.publish("orders-dlq", 0, "k-after", null, "after");
        List<Message> batch = broker.poll("g-ops", "orders-dlq", 0, 10);
        assertEquals(List.of(2L, 3L), batch.stream().map(Message::offset).toList());
        assertEquals(1, batch.get(0).attempts());
        log.info("拉取 key=k-poison(重放)/k-after offsets=2,3 判定=重放消息可正常消费且不阻塞后续位点");

        broker.commit("g-ops", "orders-dlq", 0, 2);
        broker.commit("g-ops", "orders-dlq", 0, 3);
        assertEquals(4L, broker.committedOffset("g-ops", "orders-dlq", 0));
        log.info("提交 key=k-poison offset=2 与 key=k-after offset=3 判定=重放消息正常确认, committed=4");

        // 已重放且未再进死信的消息，重复重放被拒绝且原因可区分
        ReplayResult again = broker.replayDeadLetter("orders-dlq", poison.id());
        assertFalse(again.success());
        assertEquals(ReplayRejectionReason.ALREADY_REPLAYED, again.reason());
        log.info("重复重放 key=k-poison id={} 判定=拒绝 ALREADY_REPLAYED（当前不在死信队列）", poison.id());
    }

    @Test
    void replayedMessageFailingAgainReentersDeadLetterAsSingleEntry() {
        LocalMessageBroker broker = new LocalMessageBroker(2, 0L, Clock.systemUTC());
        broker.createTopic("flaky", 1);
        broker.subscribe("g-flaky", "flaky");
        Message poison = broker.publish("flaky", 0, "k-flaky", null, "x").message();

        broker.poll("g-flaky", "flaky", 0, 10);
        driveToDeadLetter(broker, "g-flaky", "flaky", 0, 0, 2);
        assertEquals(1, broker.deadLetters("flaky").size());
        log.info("首次重试耗尽 key=k-flaky offset=0 判定=进入死信 cumulative=1");

        ReplayResult replayed = broker.replayDeadLetter("flaky", poison.id());
        assertTrue(replayed.success());
        log.info("重放 key=k-flaky id={} 新offset=1 判定=重新入队", poison.id());

        // 重放后依旧失败，重试耗尽再次进入死信：同一消息 ID 不得出现两条
        broker.poll("g-flaky", "flaky", 0, 10);
        Message deadAgain = driveToDeadLetter(broker, "g-flaky", "flaky", 0, 1, 2);
        assertEquals(poison.id(), deadAgain.id());
        List<DeadLetter> dlq = broker.deadLetters("flaky");
        assertEquals(1, dlq.size());
        assertEquals(poison.id(), dlq.get(0).message().id());
        assertEquals(1L, dlq.get(0).message().offset());
        DeadLetterStats stats = broker.deadLetterStats("flaky");
        assertEquals(1, stats.currentCount());
        assertEquals(2, stats.cumulativeCount());
        log.info("重放后再次耗尽 key=k-flaky id={} 判定=死信中仍只有一条(新offset=1), current=1 cumulative=2",
                poison.id());

        // 这次可以再次重放（当前确实在死信队列中）
        assertTrue(broker.replayDeadLetter("flaky", poison.id()).success());
        log.info("再次重放 key=k-flaky id={} 判定=允许（当前在死信队列中）", poison.id());
    }

    @Test
    void replayWithDedupKeyKeepsDedupIndexConsistent() {
        LocalMessageBroker broker = new LocalMessageBroker(2, 0L, Clock.systemUTC());
        broker.createTopic("orders-dedup", 1);
        broker.subscribe("g-dedup", "orders-dedup");

        // 带去重键发布：首次 ACCEPTED，重复发布幂等到同一条
        Message first = broker.publish("orders-dedup", 0, "k-dedup", "d-1", "v1").message();
        assertEquals(0L, first.offset());
        PublishResult dup = broker.publish("orders-dedup", 0, "k-dedup-copy", "d-1", "v1");
        assertEquals(PublishOutcome.DEDUPLICATED, dup.outcome());
        assertEquals(0L, dup.message().offset());
        log.info("去重键=d-1 重复发布 判定=幂等到 key=k-dedup offset=0, logEnd=1");

        // 重试耗尽进死信
        broker.poll("g-dedup", "orders-dedup", 0, 10);
        Message dead = driveToDeadLetter(broker, "g-dedup", "orders-dedup", 0, 0, 2);
        assertEquals(2, dead.attempts());
        assertEquals(1, broker.deadLetters("orders-dedup").size());
        assertEquals(1L, broker.committedOffset("g-dedup", "orders-dedup", 0));
        log.info("重试耗尽 key=k-dedup 去重键=d-1 offset=0 判定=进入死信, committed=1 已推进");

        // 重放：投回原分区日志末尾，去重键改指新位点
        ReplayResult replayed = broker.replayDeadLetter("orders-dedup", first.id());
        assertTrue(replayed.success());
        assertEquals(first.id(), replayed.message().id());
        assertEquals("d-1", replayed.message().dedupKey());
        assertEquals(1L, replayed.message().offset());
        assertEquals(2L, broker.logEndOffset("orders-dedup", 0));
        assertEquals(1L, broker.committedOffset("g-dedup", "orders-dedup", 0));
        log.info("重放 key=k-dedup 去重键=d-1 id={} 新offset=1 判定=去重索引改指新位点, committed 保持 1 不回退",
                first.id());

        // 再消费：重放消息能被正常拉到并确认
        List<Message> batch = broker.poll("g-dedup", "orders-dedup", 0, 10);
        assertEquals(1, batch.size());
        assertEquals(1L, batch.get(0).offset());
        assertEquals(first.id(), batch.get(0).id());
        assertEquals(1, batch.get(0).attempts());
        broker.commit("g-dedup", "orders-dedup", 0, 1);
        assertEquals(2L, broker.committedOffset("g-dedup", "orders-dedup", 0));
        log.info("再消费 key=k-dedup 去重键=d-1 offset=1 判定=重放消息正常投递并确认, committed=2");

        // 再发布：同一去重键幂等到重放后的这条有效消息，不得指回已进过死信的旧位点，也不再追加新消息
        PublishResult after = broker.publish("orders-dedup", 0, "k-dedup-new", "d-1", "v2");
        assertEquals(PublishOutcome.DEDUPLICATED, after.outcome());
        assertEquals(first.id(), after.message().id());
        assertEquals(1L, after.message().offset());
        assertEquals("v1", after.message().payload());
        assertEquals(2L, broker.logEndOffset("orders-dedup", 0));
        log.info("再发布 去重键=d-1 判定=幂等到重放消息 offset=1 payload=v1（非旧死信位点0）, logEnd 保持 2 不追加");

        // 去重口径自洽：同一分区内同一去重键只对应一条有效消息（重放后的这条）
        List<Message> tail = broker.poll("g-dedup", "orders-dedup", 0, 10);
        assertTrue(tail.isEmpty(), "全部位点已确认，不应再有投递");
        log.info("最终核对 去重键=d-1 判定=分区内仅 offset=1 一条有效消息, committed=2=logEnd 无残留投递");
    }

    @Test
    void replayRejectionsAreDistinguishable() {
        LocalMessageBroker broker = new LocalMessageBroker(1, 0L, Clock.systemUTC());
        broker.createTopic("topic-a", 1);
        broker.createTopic("topic-b", 1);
        broker.subscribe("g-a", "topic-a");
        broker.subscribe("g-b", "topic-b");

        // 从未出现过的消息 ID
        ReplayResult unknown = broker.replayDeadLetter("topic-a", "no-such-id");
        assertEquals(ReplayRejectionReason.UNKNOWN_MESSAGE, unknown.reason());
        log.info("重放 id=no-such-id topic=topic-a 判定=拒绝 UNKNOWN_MESSAGE（从未进入该主题死信）");

        // 属于其它主题的死信
        Message poisonB = broker.publish("topic-b", 0, "k-b", null, "b").message();
        broker.poll("g-b", "topic-b", 0, 10);
        broker.nack("g-b", "topic-b", 0, 0);
        assertEquals(1, broker.deadLetters("topic-b").size());
        ReplayResult mismatch = broker.replayDeadLetter("topic-a", poisonB.id());
        assertEquals(ReplayRejectionReason.TOPIC_MISMATCH, mismatch.reason());
        log.info("重放 key=k-b id={} topic=topic-a 判定=拒绝 TOPIC_MISMATCH（死信属于 topic-b）", poisonB.id());

        // 已被清理的死信
        assertTrue(broker.purgeDeadLetter("topic-b", poisonB.id()));
        assertTrue(broker.deadLetter(poisonB.id()).isEmpty());
        ReplayResult purged = broker.replayDeadLetter("topic-b", poisonB.id());
        assertEquals(ReplayRejectionReason.ALREADY_PURGED, purged.reason());
        log.info("重放 key=k-b id={} topic=topic-b 判定=拒绝 ALREADY_PURGED（已被清理）", poisonB.id());

        // 不存在的主题
        assertThrows(IllegalArgumentException.class,
                () -> broker.replayDeadLetter("topic-x", poisonB.id()));
        log.info("重放 topic=topic-x 判定=抛出异常（主题不存在）");
    }

    @Test
    void concurrentDeadLettersAcrossPartitionsKeepConsistentView() throws InterruptedException {
        LocalMessageBroker broker = new LocalMessageBroker(3, 0L, Clock.systemUTC());
        String topic = "dlq-concurrent";
        int partitions = 4;
        int perPartition = 50;
        broker.createTopic(topic, partitions);
        broker.subscribe("g-c", topic);
        for (int p = 0; p < partitions; p++) {
            for (int i = 0; i < perPartition; i++) {
                broker.publish(topic, p, "k-" + p + "-" + i, null, "body");
            }
        }
        int total = partitions * perPartition;
        log.info("并发死信准备 topic={} partitions={} 每分区={} 判定=全部作为毒消息注入", topic, partitions, perPartition);

        // 读线程：并发查询列表/统计/单查，不得报错
        AtomicBoolean running = new AtomicBoolean(true);
        ConcurrentLinkedQueue<Throwable> readErrors = new ConcurrentLinkedQueue<>();
        Thread reader = new Thread(() -> {
            while (running.get()) {
                try {
                    List<DeadLetter> snapshot = broker.deadLetters(topic);
                    DeadLetterStats stats = broker.deadLetterStats(topic);
                    assertTrue(stats.currentCount() >= 0 && stats.cumulativeCount() >= 0,
                            "统计计数不得为负");
                    for (DeadLetter dl : snapshot) {
                        broker.deadLetter(dl.message().id());
                    }
                } catch (Throwable t) {
                    readErrors.add(t);
                    return;
                }
            }
        });

        // 每分区一个消费线程：全部 nack 至死信
        ExecutorService pool = Executors.newFixedThreadPool(partitions);
        CountDownLatch start = new CountDownLatch(1);
        for (int p = 0; p < partitions; p++) {
            final int partition = p;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                int idleRounds = 0;
                while (broker.committedOffset("g-c", topic, partition) < perPartition) {
                    List<Message> msgs = broker.poll("g-c", topic, partition, 8);
                    if (msgs.isEmpty()) {
                        try {
                            Thread.sleep(2L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        assertTrue(++idleRounds < 5000, "分区 " + partition + " 消费未收敛");
                        continue;
                    }
                    idleRounds = 0;
                    for (Message m : msgs) {
                        Message r = broker.nack("g-c", topic, partition, m.offset());
                        log.info("消费失败 key={} partition={} offset={} attempts={} 判定=毒消息 nack{}",
                                m.key(), partition, m.offset(), r.attempts(),
                                r.attempts() >= 3 ? " 重试耗尽进入死信" : " 进入退避");
                    }
                }
            });
        }
        reader.start();
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "并发死信必须在超时前收敛");
        running.set(false);
        reader.join(10_000L);
        assertTrue(readErrors.isEmpty(), "并发查询不得报错: " + readErrors);

        // 视图一致性：列表 / 单查 / 当前条数 / 累计条数 必须对得上
        List<DeadLetter> all = broker.deadLetters(topic);
        assertEquals(total, all.size(), "列表条数必须等于产生的死信总数");
        Set<String> ids = new HashSet<>();
        Set<String> positions = new HashSet<>();
        for (DeadLetter dl : all) {
            assertTrue(ids.add(dl.message().id()), "死信队列不得出现同一消息的重复条目: " + dl.message().id());
            assertTrue(positions.add(dl.message().partition() + ":" + dl.message().offset()),
                    "同一位点不得重复进入死信");
            DeadLetter byId = broker.deadLetter(dl.message().id()).orElseThrow(
                    () -> new AssertionError("列表中存在但按 ID 查不到: " + dl.message().id()));
            assertEquals(dl.message().id(), byId.message().id());
        }
        DeadLetterStats stats = broker.deadLetterStats(topic);
        assertEquals(total, stats.currentCount(), "当前条数必须与列表一致");
        assertEquals(total, stats.cumulativeCount(), "累计条数必须等于进入死信的总次数");
        for (int p = 0; p < partitions; p++) {
            assertEquals(perPartition, broker.committedOffset("g-c", topic, p),
                    "死信位点视为已解决，各分区 committed 必须推进到末尾");
        }
        log.info("并发死信收敛 total={} current={} cumulative={} 判定=列表/单查/当前/累计四处口径一致, 无重复条目",
                total, stats.currentCount(), stats.cumulativeCount());
    }

    @Test
    void retentionEvictsOldestAndPurgeRemovesWithoutTouchingCumulative() {
        LocalMessageBroker broker = new LocalMessageBroker(1, 0L, Clock.systemUTC());
        String topic = "dlq-retain";
        broker.createTopic(topic, 1);
        broker.subscribe("g-r", topic);
        broker.setDeadLetterRetention(topic, 2);
        assertEquals(2, broker.deadLetterStats(topic).maxRetained());
        log.info("设置保留上限 topic={} maxRetained=2 判定=生效", topic);

        String[] ids = new String[3];
        for (int i = 0; i < 3; i++) {
            ids[i] = broker.publish(topic, 0, "k-" + i, null, "p" + i).message().id();
        }
        broker.poll("g-r", topic, 0, 10);
        for (int i = 0; i < 3; i++) {
            broker.nack("g-r", topic, 0, i);
            log.info("重试耗尽 key=k-{} offset={} id={} 判定=进入死信", i, i, ids[i]);
        }

        // 超出上限：按进入先后淘汰最旧的 k-0
        List<DeadLetter> dlq = broker.deadLetters(topic);
        assertEquals(2, dlq.size());
        assertEquals(List.of("k-1", "k-2"), dlq.stream().map(dl -> dl.message().key()).toList());
        DeadLetterStats stats = broker.deadLetterStats(topic);
        assertEquals(2, stats.currentCount());
        assertEquals(3, stats.cumulativeCount());
        assertEquals(3L, broker.committedOffset("g-r", topic, 0));
        log.info("保留上限淘汰 key=k-0 offset=0 判定=按进入先后淘汰最旧, 列表=[k-1,k-2], current=2 cumulative=3, committed=3 不受影响");

        // 被淘汰的死信视同已清理，重放拒绝
        ReplayResult evicted = broker.replayDeadLetter(topic, ids[0]);
        assertEquals(ReplayRejectionReason.ALREADY_PURGED, evicted.reason());
        assertTrue(broker.deadLetter(ids[0]).isEmpty());
        log.info("重放 key=k-0 id={} 判定=拒绝 ALREADY_PURGED（已被淘汰）", ids[0]);

        // 主动清理：查询不再返回，其余条目与累计计数不受影响
        assertTrue(broker.purgeDeadLetter(topic, ids[1]));
        assertFalse(broker.purgeDeadLetter(topic, ids[1]));
        List<DeadLetter> afterPurge = broker.deadLetters(topic);
        assertEquals(1, afterPurge.size());
        assertEquals("k-2", afterPurge.get(0).message().key());
        assertTrue(broker.deadLetter(ids[1]).isEmpty());
        assertEquals(3, broker.deadLetterStats(topic).cumulativeCount());
        log.info("主动清理 key=k-1 id={} 判定=列表与单查均不再返回, 剩余=[k-2], cumulative 保持 3", ids[1]);

        // 调低上限立即淘汰；取消上限后不再淘汰
        broker.setDeadLetterRetention(topic, 1);
        assertEquals(1, broker.deadLetters(topic).size());
        broker.setDeadLetterRetention(topic, null);
        assertEquals(null, broker.deadLetterStats(topic).maxRetained());
        broker.publish(topic, 0, "k-3", null, "p3");
        broker.publish(topic, 0, "k-4", null, "p4");
        broker.poll("g-r", topic, 0, 10);
        broker.nack("g-r", topic, 0, 3);
        broker.nack("g-r", topic, 0, 4);
        assertEquals(3, broker.deadLetters(topic).size());
        assertEquals(5, broker.deadLetterStats(topic).cumulativeCount());
        log.info("取消上限后 key=k-3/k-4 进入死信 判定=不再淘汰, current=3 cumulative=5");

        assertThrows(IllegalArgumentException.class, () -> broker.setDeadLetterRetention(topic, 0));
        log.info("设置 maxRetained=0 判定=抛出异常（上限必须 >= 1 或为 null）");
    }
}
