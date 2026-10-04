package com.github.xingjianluo.localmessagequeuejava.tests;

import com.github.xingjianluo.localmessagequeuejava.broker.DeadLetter;
import com.github.xingjianluo.localmessagequeuejava.broker.DeadLetterStats;
import com.github.xingjianluo.localmessagequeuejava.broker.LocalMessageBroker;
import com.github.xingjianluo.localmessagequeuejava.broker.Message;
import com.github.xingjianluo.localmessagequeuejava.broker.ReplayRejectionReason;
import com.github.xingjianluo.localmessagequeuejava.broker.ReplayResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 死信运维能力测试：重放、拒绝原因、统计口径、保留淘汰与主动清理。
 * 关键判定均打印「消息键 / 位点 / 判定依据」。
 */
public class DeadLetterOpsTests {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterOpsTests.class);

    /** 可手动推进的时钟，让进入死信的时间戳可区分。 */
    static final class MutableClock extends Clock {
        private volatile long millis = 1_000_000L;

        void advance(long deltaMillis) {
            this.millis += deltaMillis;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }

    /** 让指定消息持续失败直至进入死信，返回死信条目。 */
    private static DeadLetter driveToDeadLetter(LocalMessageBroker broker, String group,
                                                String topic, int partition, long offset) {
        Message m = broker.nack(group, topic, partition, offset);
        while (m.attempts() < 3) {
            m = broker.poll(group, topic, partition, 10).stream()
                    .filter(x -> x.offset() == offset).findFirst().orElseThrow();
            m = broker.nack(group, topic, partition, offset);
        }
        Message dead = m;
        DeadLetter dl = broker.deadLetter(dead.id()).orElseThrow();
        log.info("重试耗尽 key={} offset={} attempts={} 判定=进入死信 id={}", dead.key(), offset,
                dead.attempts(), dead.id());
        return dl;
    }

    @Test
    void replayRedeliversToOriginalPartitionWithoutRegressingOffsets() {
        LocalMessageBroker broker = new LocalMessageBroker(3, 0L, Clock.systemUTC());
        broker.createTopic("orders", 1);
        broker.subscribe("g1", "orders");
        broker.publish("orders", 0, "poison", null, "bad");
        broker.publish("orders", 0, "normal", null, "ok");

        broker.poll("g1", "orders", 0, 10);
        DeadLetter dl = driveToDeadLetter(broker, "g1", "orders", 0, 0);
        broker.commit("g1", "orders", 0, 1);
        assertEquals(2L, broker.committedOffset("g1", "orders", 0));
        log.info("提交 key=normal offset=1 判定=committed 推进到 2（死信位点已解决）");

        // 重放：同一消息 ID、新位点 2，回到原分区
        ReplayResult replay = broker.replayDeadLetter("orders", dl.message().id());
        assertTrue(replay.success());
        assertEquals(dl.message().id(), replay.message().id());
        assertEquals(2L, replay.message().offset());
        assertEquals(0, replay.message().partition());
        log.info("重放 key={} id={} 新offset={} 判定=投回原分区末尾, 消息ID不变",
                replay.message().key(), replay.message().id(), replay.message().offset());

        // 重放后死信清空、位点不回退、统计口径一致
        assertTrue(broker.deadLetters("orders").isEmpty());
        assertEquals(2L, broker.committedOffset("g1", "orders", 0));
        DeadLetterStats stats = broker.deadLetterStats("orders");
        assertEquals(0, stats.current());
        assertEquals(1, stats.totalEntered());
        log.info("重放后 判定=死信列表清空, committed 保持 2 不回退, current={} totalEntered={}",
                stats.current(), stats.totalEntered());

        // 重放消息按现有规则重新投递，这次消费成功
        Message redelivered = broker.poll("g1", "orders", 0, 10).get(0);
        assertEquals(2L, redelivered.offset());
        assertEquals(1, redelivered.attempts());
        assertEquals(dl.message().id(), redelivered.id());
        broker.commit("g1", "orders", 0, 2);
        assertEquals(3L, broker.committedOffset("g1", "orders", 0));
        log.info("重放后消费 key={} offset=2 attempts=1 判定=正常投递并提交, committed=3", redelivered.key());
    }

    @Test
    void replayedMessageFailingAgainReentersDeadLetterOnlyOnce() {
        LocalMessageBroker broker = new LocalMessageBroker(3, 0L, Clock.systemUTC());
        broker.createTopic("orders", 1);
        broker.subscribe("g1", "orders");
        broker.publish("orders", 0, "poison", null, "bad");

        broker.poll("g1", "orders", 0, 10);
        DeadLetter first = driveToDeadLetter(broker, "g1", "orders", 0, 0);
        ReplayResult replay = broker.replayDeadLetter("orders", first.message().id());
        assertTrue(replay.success());

        // 重放副本继续失败直至再次耗尽
        broker.poll("g1", "orders", 0, 10);
        DeadLetter second = driveToDeadLetter(broker, "g1", "orders", 0, replay.message().offset());

        List<DeadLetter> dlq = broker.deadLetters("orders");
        assertEquals(1, dlq.size(), "同一条消息在死信里不能变成两条");
        assertEquals(first.message().id(), dlq.get(0).message().id());
        assertEquals(replay.message().offset(), dlq.get(0).message().offset());
        DeadLetterStats stats = broker.deadLetterStats("orders");
        assertEquals(1, stats.current());
        assertEquals(2, stats.totalEntered());
        log.info("再次耗尽 key={} 新offset={} 判定=死信仍只有一条(同ID), current={} totalEntered={}",
                second.message().key(), second.message().offset(), stats.current(), stats.totalEntered());
        assertEquals(2L, broker.committedOffset("g1", "orders", 0));
    }

    @Test
    void replayRejectionsHaveDistinguishableReasons() {
        LocalMessageBroker broker = new LocalMessageBroker(3, 0L, Clock.systemUTC());
        broker.createTopic("orders", 1);
        broker.createTopic("payments", 1);
        broker.subscribe("g1", "orders");
        broker.publish("orders", 0, "poison", null, "bad");
        broker.poll("g1", "orders", 0, 10);
        DeadLetter dl = driveToDeadLetter(broker, "g1", "orders", 0, 0);

        ReplayResult notFound = broker.replayDeadLetter("orders", "no-such-id");
        assertFalse(notFound.success());
        assertEquals(ReplayRejectionReason.DEAD_LETTER_NOT_FOUND, notFound.reason());
        log.info("重放 id=no-such-id 判定=拒绝 DEAD_LETTER_NOT_FOUND（从未进入死信）");

        ReplayResult wrongTopic = broker.replayDeadLetter("payments", dl.message().id());
        assertFalse(wrongTopic.success());
        assertEquals(ReplayRejectionReason.TOPIC_MISMATCH, wrongTopic.reason());
        log.info("重放 key={} id={} topic=payments 判定=拒绝 TOPIC_MISMATCH（死信属于 orders）",
                dl.message().key(), dl.message().id());

        assertTrue(broker.purgeDeadLetter("orders", dl.message().id()));
        ReplayResult purged = broker.replayDeadLetter("orders", dl.message().id());
        assertFalse(purged.success());
        assertEquals(ReplayRejectionReason.DEAD_LETTER_PURGED, purged.reason());
        log.info("重放 key={} id={} 判定=拒绝 DEAD_LETTER_PURGED（死信已被清理）",
                dl.message().key(), dl.message().id());
    }

    @Test
    void concurrentMultiPartitionDeadLettersKeepViewConsistent() throws InterruptedException {
        LocalMessageBroker broker = new LocalMessageBroker(3, 0L, Clock.systemUTC());
        int partitions = 4;
        int perPartition = 60;
        broker.createTopic("tasks", partitions);
        broker.subscribe("g-w", "tasks");
        // 每个分区隔 6 条埋一条毒消息
        java.util.Set<Long> poison = new java.util.HashSet<>();
        for (long o = 0; o < perPartition; o += 6) {
            poison.add(o);
        }
        for (int p = 0; p < partitions; p++) {
            for (int i = 0; i < perPartition; i++) {
                broker.publish("tasks", p, "t-" + p + "-" + i, null, "body");
            }
        }
        int expectedDead = partitions * poison.size();

        java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicReference<AssertionError> viewError =
                new java.util.concurrent.atomic.AtomicReference<>();
        // 读线程：并发校验「列表 / 单查 / 统计」三处口径一致
        Thread reader = new Thread(() -> {
            while (running.get()) {
                try {
                    // 两次统计夹住一次列表快照：期间无新死信进入时，三处口径必须完全对齐
                    DeadLetterStats before = broker.deadLetterStats("tasks");
                    List<DeadLetter> list = broker.deadLetters("tasks");
                    DeadLetterStats stats = broker.deadLetterStats("tasks");
                    if (before.totalEntered() != stats.totalEntered()) {
                        continue; // 快照期间有新死信进入，本轮不可比，重取
                    }
                    assertEquals(list.size(), stats.current(), "列表条数必须等于统计当前值");
                    assertEquals(before.current(), stats.current(), "无变更窗口内当前条数必须稳定");
                    assertTrue(stats.totalEntered() >= stats.current(), "累计进入不得小于当前条数");
                    java.util.Set<String> ids = new java.util.HashSet<>();
                    for (DeadLetter dl : list) {
                        assertTrue(ids.add(dl.message().id()), "死信列表不得出现重复消息ID");
                        DeadLetter byId = broker.deadLetter(dl.message().id()).orElseThrow(
                                () -> new AssertionError("列表中的死信必须能按ID查到: " + dl.message().id()));
                        assertEquals(dl.message().offset(), byId.message().offset(),
                                "单查结果必须与列表一致");
                        assertEquals("tasks", byId.message().topic());
                    }
                } catch (AssertionError e) {
                    viewError.compareAndSet(null, e);
                    return;
                }
            }
        });
        reader.start();

        // 每个分区一个消费线程：毒消息持续 nack 至死信，正常消息提交
        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(partitions);
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        for (int p = 0; p < partitions; p++) {
            final int partition = p;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                while (broker.committedOffset("g-w", "tasks", partition) < perPartition) {
                    List<Message> msgs = broker.poll("g-w", "tasks", partition, 8);
                    for (Message m : msgs) {
                        if (poison.contains(m.offset())) {
                            broker.nack("g-w", "tasks", partition, m.offset());
                        } else {
                            broker.commit("g-w", "tasks", partition, m.offset());
                        }
                    }
                    if (msgs.isEmpty()) {
                        try {
                            Thread.sleep(2L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS), "并发消费必须收敛");
        running.set(false);
        reader.join(5000L);
        if (viewError.get() != null) {
            throw viewError.get();
        }

        // 收敛后终态校验：三处口径对齐，每个分区 committed 到位
        List<DeadLetter> list = broker.deadLetters("tasks");
        DeadLetterStats stats = broker.deadLetterStats("tasks");
        assertEquals(expectedDead, list.size());
        assertEquals(expectedDead, stats.current());
        assertEquals(expectedDead, stats.totalEntered());
        java.util.Set<String> ids = new java.util.HashSet<>();
        for (DeadLetter dl : list) {
            assertTrue(ids.add(dl.message().id()));
            assertTrue(broker.deadLetter(dl.message().id()).isPresent());
        }
        for (int p = 0; p < partitions; p++) {
            assertEquals(perPartition, broker.committedOffset("g-w", "tasks", p));
        }
        log.info("多分区并发死信 partitions={} 每分区毒消息={} 判定=列表/单查/统计一致 current={} totalEntered={} 各分区committed到位",
                partitions, poison.size(), stats.current(), stats.totalEntered());
    }

    @Test
    void retentionEvictsOldestAndPurgeRemovesWithoutTouchingTotals() {
        MutableClock clock = new MutableClock();
        LocalMessageBroker broker = new LocalMessageBroker(3, 0L, clock);
        broker.createTopic("emails", 1);
        broker.subscribe("g1", "emails");
        broker.setDeadLetterRetention("emails", 3);

        // 制造 5 条死信，时间戳依次递增，进入先后可区分
        java.util.List<String> deadIds = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            broker.publish("emails", 0, "mail-" + i, null, "body-" + i);
            broker.poll("g1", "emails", 0, 10);
            DeadLetter dl = driveToDeadLetter(broker, "g1", "emails", 0, i);
            deadIds.add(dl.message().id());
            clock.advance(10);
        }

        // 保留上限 3：最旧的 mail-0 / mail-1 被淘汰，留下墓碑
        List<DeadLetter> dlq = broker.deadLetters("emails");
        assertEquals(3, dlq.size());
        assertEquals(java.util.List.of("mail-2", "mail-3", "mail-4"),
                dlq.stream().map(d -> d.message().key()).toList());
        DeadLetterStats stats = broker.deadLetterStats("emails");
        assertEquals(3, stats.current());
        assertEquals(5, stats.totalEntered());
        assertEquals(3, stats.retentionLimit());
        log.info("保留上限=3 进入5条 判定=按进入先后淘汰最旧 mail-0/mail-1, 存活={}, current={} totalEntered={}",
                dlq.stream().map(d -> d.message().key()).toList(), stats.current(), stats.totalEntered());

        // 淘汰不影响正常分区位点推进
        assertEquals(5L, broker.committedOffset("g1", "emails", 0));
        assertEquals(5L, broker.logEndOffset("emails", 0));
        log.info("淘汰后 committed={} logEnd={} 判定=位点推进不受淘汰影响",
                broker.committedOffset("g1", "emails", 0), broker.logEndOffset("emails", 0));

        // 被淘汰的死信：单查不到、重放给出 PURGED
        assertTrue(broker.deadLetter(deadIds.get(0)).isEmpty());
        ReplayResult evicted = broker.replayDeadLetter("emails", deadIds.get(0));
        assertFalse(evicted.success());
        assertEquals(ReplayRejectionReason.DEAD_LETTER_PURGED, evicted.reason());
        log.info("重放被淘汰 key=mail-0 id={} 判定=拒绝 DEAD_LETTER_PURGED", deadIds.get(0));

        // 主动清理 mail-2：查询不再返回，累计计数与其余死信不受影响
        assertTrue(broker.purgeDeadLetter("emails", deadIds.get(2)));
        assertTrue(broker.deadLetter(deadIds.get(2)).isEmpty());
        assertEquals(java.util.List.of("mail-3", "mail-4"),
                broker.deadLetters("emails").stream().map(d -> d.message().key()).toList());
        DeadLetterStats afterPurge = broker.deadLetterStats("emails");
        assertEquals(2, afterPurge.current());
        assertEquals(5, afterPurge.totalEntered());
        log.info("清理 key=mail-2 id={} 判定=列表/单查不再返回, current={} totalEntered={} 不变",
                deadIds.get(2), afterPurge.current(), afterPurge.totalEntered());

        // 重复清理 / 跨主题清理均失败；清理后可重放拒绝原因仍是 PURGED
        assertFalse(broker.purgeDeadLetter("emails", deadIds.get(2)));
        broker.createTopic("other", 1);
        assertFalse(broker.purgeDeadLetter("other", deadIds.get(3)));
        assertEquals(ReplayRejectionReason.DEAD_LETTER_PURGED,
                broker.replayDeadLetter("emails", deadIds.get(2)).reason());
        log.info("重复清理/跨主题清理 判定=返回 false, 已清理消息重放仍拒绝 DEAD_LETTER_PURGED");

        // 调低上限立即对存量生效；设为 -1 取消限制
        broker.setDeadLetterRetention("emails", 1);
        assertEquals(java.util.List.of("mail-4"),
                broker.deadLetters("emails").stream().map(d -> d.message().key()).toList());
        broker.setDeadLetterRetention("emails", -1);
        assertEquals(-1, broker.deadLetterStats("emails").retentionLimit());
        assertEquals(5, broker.deadLetterStats("emails").totalEntered());
        log.info("调低上限=1 判定=立即淘汰存量至最新1条; 上限=-1 判定=取消限制, totalEntered 仍为 5");
    }
}
