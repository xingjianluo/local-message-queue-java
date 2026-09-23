package com.github.xingjianluo.localmessagequeuejava.tests;

import com.github.xingjianluo.localmessagequeuejava.broker.CommitRejectionReason;
import com.github.xingjianluo.localmessagequeuejava.broker.CommitResult;
import com.github.xingjianluo.localmessagequeuejava.broker.DeadLetter;
import com.github.xingjianluo.localmessagequeuejava.broker.LocalMessageBroker;
import com.github.xingjianluo.localmessagequeuejava.broker.Message;
import com.github.xingjianluo.localmessagequeuejava.broker.PublishOutcome;
import com.github.xingjianluo.localmessagequeuejava.broker.PublishResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地消息队列核心语义测试：重复发布、位点管理、重试退避、死信与并发一致性。
 * 关键判定均打印「消息键 / 位点 / 判定依据」。
 */
public class LocalMessageBrokerTests {

    private static final Logger log = LoggerFactory.getLogger(LocalMessageBrokerTests.class);

    /** 可手动推进的时钟，用于确定性地验证退避。 */
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

    @Test
    void publishAndConsumeInPartitionOrderWithStableOffsets() {
        LocalMessageBroker broker = new LocalMessageBroker();
        broker.createTopic("orders", 2);
        broker.subscribe("g-audit", "orders");

        for (int i = 0; i < 3; i++) {
            PublishResult r = broker.publish("orders", 0, "key-" + i, null, "payload-" + i);
            assertEquals(PublishOutcome.ACCEPTED, r.outcome());
            assertEquals(i, r.message().offset());
            log.info("发布 key={} offset={} 判定=ACCEPTED 首次写入", r.message().key(), r.message().offset());
        }
        assertEquals(3, broker.logEndOffset("orders", 0));

        List<Message> batch = broker.poll("g-audit", "orders", 0, 10);
        assertEquals(List.of(0L, 1L, 2L), batch.stream().map(Message::offset).toList());
        log.info("拉取 key=key-0/key-1/key-2 offsets=0,1,2 判定=同分区严格按位点顺序投递");

        // 乱序确认：先确认 1，committed 不得越过未解决的 0
        CommitResult later = broker.commit("g-audit", "orders", 0, 1);
        assertTrue(later.success());
        assertEquals(0L, later.committed());
        log.info("提交 key=key-1 offset=1 判定=乱序确认暂存, committed 仍为 0（offset 0 未解决）");
        CommitResult head = broker.commit("g-audit", "orders", 0, 0);
        assertEquals(2L, head.committed());
        log.info("提交 key=key-0 offset=0 判定=连续位点补齐, committed 自动推进到 {}", head.committed());

        // 重复确认 / 重放不得使位点回退
        CommitResult replay = broker.commit("g-audit", "orders", 0, 0);
        assertFalse(replay.success());
        assertEquals(CommitRejectionReason.OFFSET_REGRESSION, replay.reason());
        log.info("重放 key=key-0 offset=0 判定=拒绝 OFFSET_REGRESSION, committed 保持 {}", replay.committed());
        assertTrue(broker.poll("g-audit", "orders", 0, 10).isEmpty());
    }

    @Test
    void commitRejectionsHaveDistinguishableReasons() {
        LocalMessageBroker broker = new LocalMessageBroker();
        broker.createTopic("orders", 1);
        broker.subscribe("g-audit", "orders");
        broker.publish("orders", 0, "key-0", null, "p0");
        broker.poll("g-audit", "orders", 0, 10);
        broker.commit("g-audit", "orders", 0, 0);

        CommitResult regression = broker.commit("g-audit", "orders", 0, 0);
        assertEquals(CommitRejectionReason.OFFSET_REGRESSION, regression.reason());
        log.info("提交 key=key-0 offset=0 判定=拒绝 OFFSET_REGRESSION（已确认位点不回退）");

        CommitResult outOfRange = broker.commit("g-audit", "orders", 0, 5);
        assertEquals(CommitRejectionReason.OFFSET_OUT_OF_RANGE, outOfRange.reason());
        log.info("提交 key=<不存在> offset=5 判定=拒绝 OFFSET_OUT_OF_RANGE（超过最大位点 {}）",
                broker.logEndOffset("orders", 0));

        CommitResult unknownGroup = broker.commit("g-other", "orders", 0, 0);
        assertEquals(CommitRejectionReason.UNKNOWN_GROUP, unknownGroup.reason());
        log.info("提交 group=g-other key=key-0 offset=0 判定=拒绝 UNKNOWN_GROUP（位点不属于该消费者组）");

        CommitResult unknownPartition = broker.commit("g-audit", "orders", 9, 0);
        assertEquals(CommitRejectionReason.UNKNOWN_PARTITION, unknownPartition.reason());
        log.info("提交 key=key-0 offset=0 partition=9 判定=拒绝 UNKNOWN_PARTITION（分区越界）");
        assertThrows(IllegalArgumentException.class,
                () -> broker.publish("orders", 7, "x", null, "y"));
    }

    @Test
    void duplicatePublishByDedupKeyOnlyFirstTakesEffect() {
        LocalMessageBroker broker = new LocalMessageBroker();
        broker.createTopic("payments", 1);
        broker.subscribe("g-pay", "payments");

        PublishResult first = broker.publish("payments", 0, "key-A", "dedup-123", "100 元");
        PublishResult second = broker.publish("payments", 0, "key-A-copy", "dedup-123", "100 元");
        PublishResult third = broker.publish("payments", 0, "key-A-copy", "dedup-123", "100 元");

        assertEquals(PublishOutcome.ACCEPTED, first.outcome());
        assertEquals(PublishOutcome.DEDUPLICATED, second.outcome());
        assertEquals(first.message().id(), second.message().id());
        assertEquals(first.message().id(), third.message().id());
        assertEquals(1L, broker.logEndOffset("payments", 0));
        log.info("去重键=dedup-123 重复发布3次 判定=仅首次 key=key-A offset=0 生效, 后续返回同一条消息, logEnd=1");

        // 不同分区的去重键互不影响
        broker.createTopic("payments2", 2);
        broker.publish("payments2", 0, "k", "dup-x", "a");
        PublishResult otherPartition = broker.publish("payments2", 1, "k", "dup-x", "a");
        assertEquals(PublishOutcome.ACCEPTED, otherPartition.outcome());
        log.info("去重键=dup-x 发布到不同分区 判定=分区维度独立, key=k offset=0 再次 ACCEPTED");
    }

    @Test
    void failedMessageRetriesWithBackoffAndEntersDeadLetterQueue() throws InterruptedException {
        MutableClock clock = new MutableClock();
        LocalMessageBroker broker = new LocalMessageBroker(3, 100L, clock);
        broker.createTopic("emails", 1);
        broker.subscribe("g-mailer", "emails");
        broker.publish("emails", 0, "mail-0", null, "hello");
        broker.publish("emails", 0, "mail-1", null, "world");

        List<Message> first = broker.poll("g-mailer", "emails", 0, 10);
        assertEquals(List.of(0L, 1L), first.stream().map(Message::offset).toList());

        // mail-0 第一次失败：进入退避；mail-1 正常确认不受阻塞
        Message m1 = broker.nack("g-mailer", "emails", 0, 0);
        assertEquals(2, m1.attempts());
        log.info("消费失败 key=mail-0 offset=0 attempts=2 判定=进入线性退避(100ms)");
        broker.commit("g-mailer", "emails", 0, 1);
        log.info("提交 key=mail-1 offset=1 判定=单条失败不阻塞同分区其他位点, committed 暂为 {}",
                broker.committedOffset("g-mailer", "emails", 0));

        // 退避未到期：不重复投递 mail-0
        assertTrue(broker.poll("g-mailer", "emails", 0, 10).isEmpty());
        log.info("退避期内 poll key=mail-0 offset=0 判定=在途退避中, 不重复投递");

        // 二次失败的线性退避 = backoffMillis * attempts = 100 * 2
        clock.advance(201);
        Message redelivered = broker.poll("g-mailer", "emails", 0, 10).get(0);
        assertEquals(0L, redelivered.offset());
        assertEquals(2, redelivered.attempts());
        log.info("退避到期 key=mail-0 offset=0 attempts=2 判定=重新投递同一消息");

        // 再次失败达到上限 -> 死信
        Message dead = broker.nack("g-mailer", "emails", 0, 0);
        assertEquals(3, dead.attempts());
        List<DeadLetter> dlq = broker.deadLetters("emails");
        assertEquals(1, dlq.size());
        assertEquals(dead.id(), dlq.get(0).message().id());
        log.info("再次失败 key=mail-0 offset=0 attempts=3 判定=重试耗尽进入死信队列, 原因={}", dlq.get(0).reason());
        assertNotNull(broker.deadLetter(dead.id()).orElse(null));
        log.info("按消息ID查询死信 key=mail-0 id={} 判定=可单独查询", dead.id());

        // 死信位点视为已解决，committed 推进到分区末尾，重放被拒绝
        assertEquals(2L, broker.committedOffset("g-mailer", "emails", 0));
        CommitResult replay = broker.commit("g-mailer", "emails", 0, 0);
        assertEquals(CommitRejectionReason.OFFSET_REGRESSION, replay.reason());
        log.info("死信后重放 key=mail-0 offset=0 判定=拒绝 OFFSET_REGRESSION, committed=2");
        assertTrue(broker.poll("g-mailer", "emails", 0, 10).isEmpty());
    }

    @Test
    void concurrentPublishSameDedupKeyPublishesExactlyOnce() throws InterruptedException {
        LocalMessageBroker broker = new LocalMessageBroker();
        broker.createTopic("stock", 1);
        int threads = 32;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Set<String> acceptedKeys = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicated = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                PublishResult r = broker.publish("stock", 0, "k", "dedup-only", "v");
                if (r.outcome() == PublishOutcome.ACCEPTED) {
                    accepted.incrementAndGet();
                    acceptedKeys.add(r.message().id());
                } else {
                    duplicated.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(1, accepted.get(), "并发发布同一去重键只能有一次 ACCEPTED");
        assertEquals(threads - 1, duplicated.get());
        assertEquals(1, acceptedKeys.size());
        assertEquals(1L, broker.logEndOffset("stock", 0));
        log.info("并发发布去重键=dedup-only threads={} 判定=ACCEPTED={} DEDUPLICATED={} logEnd=1 唯一消息ID={}",
                threads, accepted.get(), duplicated.get(), acceptedKeys.iterator().next());
    }

    @Test
    void concurrentConsumptionKeepsOffsetsDedupAndDeadLetterConsistent() throws InterruptedException {
        LocalMessageBroker broker = new LocalMessageBroker(3, 0L, Clock.systemUTC());
        broker.createTopic("tasks", 1);
        broker.subscribe("g-workers", "tasks");
        int messageCount = 200;
        // 部分消息携带相同去重键，验证并发下重复发布被吞掉
        for (int i = 0; i < messageCount; i++) {
            String dedup = (i % 25 == 0) ? "dup-" + i : null;
            broker.publish("tasks", 0, "task-" + i, dedup, "body-" + i);
        }
        long logEnd = broker.logEndOffset("tasks", 0);
        log.info("并发消费准备 计划发布={} 实际 logEnd={} 判定=携带重复去重键的仅首次写入", messageCount, logEnd);

        // 每个位点只允许被「成功处理」一次；部分消息注入失败，部分始终失败 -> 死信
        Set<Long> succeededOffsets = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Map<Long, AtomicInteger> deliveryCount = new ConcurrentHashMap<>();
        Set<Long> poisonOffsets = ConcurrentHashMap.newKeySet();
        for (long poison = 0; poison < logEnd; poison += 40) {
            poisonOffsets.add(poison);
        }
        int consumers = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        AtomicInteger commitOk = new AtomicInteger();
        for (int t = 0; t < consumers; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                int idleRounds = 0;
                while (true) {
                    List<Message> msgs = broker.poll("g-workers", "tasks", 0, 16);
                    if (msgs.isEmpty()) {
                        if (broker.committedOffset("g-workers", "tasks", 0) >= logEnd) {
                            return;
                        }
                        // 还有消息在其他消费者手中或退避中，短暂等待后重试，避免提前退出
                        try {
                            Thread.sleep(5L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        assertTrue(++idleRounds < 1000, "消费者空转过久，流程未收敛");
                        continue;
                    }
                    idleRounds = 0;
                    for (Message m : msgs) {
                        deliveryCount.computeIfAbsent(m.offset(), k -> new AtomicInteger()).incrementAndGet();
                        if (poisonOffsets.contains(m.offset())) {
                            broker.nack("g-workers", "tasks", 0, m.offset());
                            log.info("消费失败 key={} offset={} attempts={} 判定=毒消息持续 nack 直至死信",
                                    m.key(), m.offset(), m.attempts());
                        } else {
                            assertTrue(succeededOffsets.add(m.offset()),
                                    "同一条消息不得被重复成功消费: offset=" + m.offset());
                            CommitResult cr = broker.commit("g-workers", "tasks", 0, m.offset());
                            if (cr.success()) {
                                commitOk.incrementAndGet();
                            }
                            log.info("消费成功 key={} offset={} 判定=提交成功 committed={}",
                                    m.key(), m.offset(), cr.committed());
                        }
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "并发消费必须在超时前收敛");

        assertEquals(logEnd, broker.committedOffset("g-workers", "tasks", 0),
                "并发结束后已确认位点必须等于分区最大位点");
        assertEquals(logEnd - poisonOffsets.size(), succeededOffsets.size(),
                "每条非毒消息恰好成功消费一次");
        assertEquals(poisonOffsets.size(), broker.deadLetters("tasks").size(),
                "所有毒消息必须进入死信队列");
        // 同一条消息同一时刻不会被两个消费者拿到；退避为 0，毒消息在 nack 后最多被重投 maxAttempts 次
        deliveryCount.forEach((offset, count) -> {
            int bound = poisonOffsets.contains(offset) ? 3 : 1;
            assertTrue(count.get() <= bound,
                    "offset=" + offset + " 投递次数 " + count.get() + " 超过上限 " + bound);
        });
        Set<Long> dlOffsets = new HashSet<>();
        broker.deadLetters("tasks").forEach(dl -> {
            assertEquals(3, dl.message().attempts());
            assertTrue(dlOffsets.add(dl.message().offset()), "死信队列不得出现同一消息的重复条目");
        });
        log.info("并发消费收敛 logEnd={} committed={} 成功={} 死信={} 判定=位点/去重/死信状态一致",
                logEnd, broker.committedOffset("g-workers", "tasks", 0),
                succeededOffsets.size(), broker.deadLetters("tasks").size());
    }
}
