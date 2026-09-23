package com.github.xingjianluo.localmessagequeuejava.tests;

import com.github.xingjianluo.localmessagequeuejava.mq.CommitResult;
import com.github.xingjianluo.localmessagequeuejava.mq.DeadLetter;
import com.github.xingjianluo.localmessagequeuejava.mq.LocalMessageQueue;
import com.github.xingjianluo.localmessagequeuejava.mq.Message;
import com.github.xingjianluo.localmessagequeuejava.mq.PublishResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 本地消息队列语义测试：重复发布、位点裁决、重试死信、并发消费。
 * 每条断言均打印消息键、位点与判定依据。
 */
public class LocalMessageQueueTests {

    private static final Logger log = LoggerFactory.getLogger(LocalMessageQueueTests.class);

    private static final String TOPIC = "orders";
    private static final String GROUP = "order-workers";

    @Test
    public void commitReasonsAreDistinguishableAndOffsetsNeverRegress() {
        LocalMessageQueue queue = new LocalMessageQueue(2);
        queue.createTopic(TOPIC, 1);
        queue.registerGroup(GROUP, TOPIC);

        for (int i = 0; i < 3; i++) {
            PublishResult pr = queue.publish(TOPIC, 0, "key-" + i, null, "payload-" + i);
            log.info("[发布] key={} offset={} 判定={} messageId={}",
                    pr.message().key(), pr.message().offset(), pr.status(), pr.message().messageId());
        }

        List<Message> batch = queue.poll(GROUP, TOPIC, 0, 10);
        assertEquals(3, batch.size());
        batch.forEach(m -> log.info("[投递] key={} offset={} 判定=按位点顺序首次投递", m.key(), m.offset()));

        // 越界提交
        CommitResult outOfRange = queue.commit(GROUP, TOPIC, 0, 99);
        log.info("[提交] key={} offset={} 判定={} accepted={} 当前位点={}",
                "-", 99, CommitResult.Reason.OFFSET_OUT_OF_RANGE, outOfRange.accepted(),
                outOfRange.committedOffset());
        assertFalse(outOfRange.accepted());
        assertEquals(CommitResult.Reason.OFFSET_OUT_OF_RANGE, outOfRange.reason());

        // 未注册到该主题的组提交
        CommitResult unknownGroup = queue.commit("alien-group", TOPIC, 0, 0);
        log.info("[提交] key={} offset={} 判定={} accepted={}",
                "key-0", 0, CommitResult.Reason.UNKNOWN_GROUP, unknownGroup.accepted());
        assertFalse(unknownGroup.accepted());
        assertEquals(CommitResult.Reason.UNKNOWN_GROUP, unknownGroup.reason());

        // 乱序确认 offset 1：先标记为 CONFIRMED，但水位仍停在 -1（offset 0 未终结）
        CommitResult confirmOne = queue.commit(GROUP, TOPIC, 0, batch.get(1).offset());
        log.info("[提交] key={} offset={} 判定=单条确认成功但水位不越过未终结的offset0 committed={}",
                batch.get(1).key(), batch.get(1).offset(), confirmOne.committedOffset());
        assertTrue(confirmOne.accepted());
        assertEquals(-1, confirmOne.committedOffset());

        // 确认 offset 0：0/1 均已终结，水位推进到 1
        CommitResult confirmZero = queue.commit(GROUP, TOPIC, 0, 0);
        assertEquals(1, confirmZero.committedOffset());

        // 重放已确认位点（提交与水位相等的位点）：幂等放行，不产生状态变化、不回退
        CommitResult idempotent = queue.commit(GROUP, TOPIC, 0, 1);
        log.info("[提交] key={} offset={} 判定=相等位点重放幂等放行 committed={}",
                "key-1", 1, idempotent.committedOffset());
        assertTrue(idempotent.accepted());

        // 提交比已确认水位更小的位点：回退，拒绝
        CommitResult regress = queue.commit(GROUP, TOPIC, 0, 0);
        log.info("[提交] key={} offset={} 判定={} accepted={} 当前位点={}",
                "key-0", 0, CommitResult.Reason.OFFSET_REGRESSION, regress.accepted(),
                regress.committedOffset());
        assertFalse(regress.accepted());
        assertEquals(CommitResult.Reason.OFFSET_REGRESSION, regress.reason());
        assertEquals(1, queue.committedOffset(GROUP, TOPIC, 0));

        // 补确认 offset 2，水位推进到底
        CommitResult confirmTwo = queue.commit(GROUP, TOPIC, 0, 2);
        log.info("[提交] key={} offset={} 判定=连续终结水位推进 committed={}",
                "key-2", 2, confirmTwo.committedOffset());
        assertEquals(2, confirmTwo.committedOffset());
    }

    @Test
    public void duplicatePublishByDedupKeyOnlyFirstTakesEffectUnderConcurrency() throws Exception {
        LocalMessageQueue queue = new LocalMessageQueue(2);
        queue.createTopic(TOPIC, 2);

        int publishers = 16;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(publishers);
        Set<String> firstMessageIds = ConcurrentHashMap.newKeySet();
        AtomicLong publishedCount = new AtomicLong();
        AtomicLong duplicateCount = new AtomicLong();
        try {
            for (int i = 0; i < publishers; i++) {
                pool.submit(() -> {
                    start.await();
                    PublishResult pr = queue.publish(TOPIC, 0, "same-key", "dedup-42", "same-payload");
                    if (pr.status() == PublishResult.Status.PUBLISHED) {
                        publishedCount.incrementAndGet();
                        firstMessageIds.add(pr.message().messageId());
                    } else {
                        duplicateCount.incrementAndGet();
                    }
                    log.info("[并发发布] key={} dedupKey=dedup-42 offset={} 判定={} messageId={}",
                            pr.message().key(), pr.message().offset(), pr.status(),
                            pr.message().messageId());
                    return null;
                });
            }
            start.countDown();
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }

        log.info("[并发发布结论] PUBLISHED={} DUPLICATE={} 首条messageId集合={}",
                publishedCount.get(), duplicateCount.get(), firstMessageIds);
        assertEquals(1, publishedCount.get(), "同去重键只有首次发布生效");
        assertEquals(publishers - 1, duplicateCount.get());
        assertEquals(1, firstMessageIds.size());
        assertEquals(-1, queue.committedOffset(GROUP, TOPIC, 0), "未注册组位点保持 -1");

        // 不同去重键正常产生新位点
        PublishResult second = queue.publish(TOPIC, 0, "other-key", "dedup-43", "p2");
        log.info("[发布] key={} offset={} 判定=不同去重键正常追加", second.message().key(),
                second.message().offset());
        assertEquals(1, second.message().offset());

        // 同去重键再次发布仍命中首次消息（位点不回退/不新增）
        PublishResult again = queue.publish(TOPIC, 0, "same-key", "dedup-42", "same-payload");
        assertEquals(PublishResult.Status.DUPLICATE, again.status());
        assertEquals(0, again.message().offset());
        log.info("[重复发布] key={} offset={} 判定=DUPLICATE 指向首次位点不新增消息",
                again.message().key(), again.message().offset());
    }

    @Test
    public void failedMessageRetriesWithBackoffAndEntersDeadLetterWithoutBlockingOthers() {
        AtomicLong now = new AtomicLong(1_000_000L);
        int maxRetries = 2;
        LocalMessageQueue queue = new LocalMessageQueue(maxRetries, 100L, now::get);
        queue.createTopic(TOPIC, 1);
        queue.registerGroup(GROUP, TOPIC);

        PublishResult bad = queue.publish(TOPIC, 0, "bad-key", null, "fail-payload");
        PublishResult good = queue.publish(TOPIC, 0, "good-key", null, "ok-payload");
        log.info("[发布] key={} offset={} 判定=PUBLISHED", bad.message().key(), bad.message().offset());
        log.info("[发布] key={} offset={} 判定=PUBLISHED", good.message().key(), good.message().offset());

        // 两条消息可同时在处理：offset1 先成功确认，offset0 失败中 —— 互不阻塞
        List<Message> firstPoll = queue.poll(GROUP, TOPIC, 0, 10);
        assertEquals(2, firstPoll.size());
        queue.commit(GROUP, TOPIC, 0, good.message().offset());
        log.info("[提交] key={} offset={} 判定=单条确认；offset0未终结，水位停留 committed={}",
                good.message().key(), good.message().offset(),
                queue.committedOffset(GROUP, TOPIC, 0));
        assertEquals(-1, queue.committedOffset(GROUP, TOPIC, 0));

        // 第 1 次失败：进入退避，退避期内 poll 不再投递该消息
        queue.nack(GROUP, TOPIC, 0, bad.message().offset(), "boom-1");
        List<Message> duringBackoff = queue.poll(GROUP, TOPIC, 0, 10);
        log.info("[退避期内 poll] key={} offset={} 判定=RETRY_WAIT 未到期不投递，返回条数={}",
                bad.message().key(), bad.message().offset(), duringBackoff.size());
        assertTrue(duringBackoff.isEmpty());

        // 退避到期后重投：同一条消息（messageId/offset 不变），不会被重复投递成新消息
        now.addAndGet(101L);
        List<Message> redeliver1 = queue.poll(GROUP, TOPIC, 0, 10);
        assertEquals(1, redeliver1.size());
        assertEquals(bad.message().messageId(), redeliver1.get(0).messageId());
        assertEquals(0, redeliver1.get(0).offset());
        log.info("[重试投递] key={} offset={} 判定=退避到期重投同一条消息(第2次处理) messageId={}",
                bad.message().key(), bad.message().offset(), redeliver1.get(0).messageId());

        queue.nack(GROUP, TOPIC, 0, 0, "boom-2");
        now.addAndGet(201L);
        List<Message> redeliver2 = queue.poll(GROUP, TOPIC, 0, 10);
        assertEquals(1, redeliver2.size());
        log.info("[重试投递] key={} offset={} 判定=指数退避到期重投(第3次处理)",
                bad.message().key(), redeliver2.get(0).offset());

        // 第 3 次处理仍失败：attempts(3) > maxRetries(2)，进入死信
        queue.nack(GROUP, TOPIC, 0, 0, "boom-3");
        List<DeadLetter> dead = queue.deadLetters(GROUP);
        assertEquals(1, dead.size());
        DeadLetter dl = dead.get(0);
        log.info("[死信] key={} offset={} 判定=重试{}次仍失败(lastError={})，进入死信队列 messageId={}",
                dl.message().key(), dl.message().offset(), dl.attempts(), dl.lastError(),
                dl.message().messageId());
        assertEquals("bad-key", dl.message().key());
        assertEquals(3, dl.attempts());
        assertEquals("boom-3", dl.lastError());
        assertTrue(queue.findDeadLetter(GROUP, bad.message().messageId()).isPresent());

        // 死信后不再投递
        now.addAndGet(10_000L);
        assertTrue(queue.poll(GROUP, TOPIC, 0, 10).isEmpty());

        // offset0 以 DEAD 终结、offset1 已 CONFIRMED：水位一次性推进到 1
        long committed = queue.committedOffset(GROUP, TOPIC, 0);
        log.info("[位点结论] key1={} key0={} 判定=DEAD 与 CONFIRMED 同为终结态，水位推进 committed={}",
                good.message().key(), bad.message().key(), committed);
        assertEquals(1, committed);
    }

    @Test
    public void concurrentConsumersKeepOffsetsDedupAndDeadLettersConsistent() throws Exception {
        LocalMessageQueue queue = new LocalMessageQueue(1, 1L, System::currentTimeMillis);
        queue.createTopic(TOPIC, 2);
        queue.registerGroup(GROUP, TOPIC);
        queue.registerGroup("other-group", TOPIC);

        int total = 40;
        // 偶数位点在分区0、奇数位点在分区1；固定一批“毒消息”必定失败到死信
        Set<Long> poisonOffsetsP0 = ConcurrentHashMap.newKeySet();
        Set<Long> poisonOffsetsP1 = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < total; i++) {
            int partition = i % 2;
            PublishResult pr = queue.publish(TOPIC, partition, "key-" + i,
                    "dedup-" + (i % 8), "payload-" + i);
            if (i % 7 == 0) {
                (partition == 0 ? poisonOffsetsP0 : poisonOffsetsP1).add(pr.message().offset());
            }
        }
        // 注意：去重键每 5 条复用一次，故实际入日志的消息少于 total
        long sizeP0 = queue.logSize(TOPIC, 0);
        long sizeP1 = queue.logSize(TOPIC, 1);
        log.info("[并发消费准备] 去重后分区0消息数={} 分区1消息数={}", sizeP0, sizeP1);

        ConcurrentHashMap<String, Integer> deliveriesById = new ConcurrentHashMap<>();
        int consumers = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        try {
            for (int c = 0; c < consumers; c++) {
                final int workerId = c;
                pool.submit(() -> {
                    start.await();
                    int emptyRounds = 0;
                    while (emptyRounds < 200) {
                        boolean got = false;
                        for (int p = 0; p < 2; p++) {
                            List<Message> batch = queue.poll(GROUP, TOPIC, p, 5);
                            for (Message m : batch) {
                                got = true;
                                deliveriesById.merge(m.messageId(), 1, Integer::sum);
                                Set<Long> poison = p == 0 ? poisonOffsetsP0 : poisonOffsetsP1;
                                if (poison.contains(m.offset())) {
                                    queue.nack(GROUP, TOPIC, p, m.offset(), "poison@worker" + workerId);
                                    log.info("[并发消费] worker={} key={} offset={} 判定=处理失败nack",
                                            workerId, m.key(), m.offset());
                                } else {
                                    CommitResult cr = queue.commit(GROUP, TOPIC, p, m.offset());
                                    log.info("[并发消费] worker={} key={} offset={} 判定=commit accepted={} committed={}",
                                            workerId, m.key(), m.offset(), cr.accepted(), cr.committedOffset());
                                    assertTrue(cr.accepted());
                                }
                            }
                        }
                        if (got) {
                            emptyRounds = 0;
                        } else {
                            emptyRounds++;
                            Thread.sleep(1L);
                        }
                    }
                    return null;
                });
            }
            start.countDown();
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        }

        long committedP0 = queue.committedOffset(GROUP, TOPIC, 0);
        long committedP1 = queue.committedOffset(GROUP, TOPIC, 1);
        log.info("[并发位点结论] 分区0 committed={} (末位点={}) 分区1 committed={} (末位点={})",
                committedP0, sizeP0 - 1, committedP1, sizeP1 - 1);
        assertEquals(sizeP0 - 1, committedP0);
        assertEquals(sizeP1 - 1, committedP1);

        List<DeadLetter> dead = queue.deadLetters(GROUP);
        long expectedDeadP0 = poisonOffsetsP0.stream().filter(o -> o < sizeP0).count();
        long expectedDeadP1 = poisonOffsetsP1.stream().filter(o -> o < sizeP1).count();
        log.info("[并发死信结论] 死信总数={} 期望(分区0={}+分区1={})={}",
                dead.size(), expectedDeadP0, expectedDeadP1, expectedDeadP0 + expectedDeadP1);
        assertEquals(expectedDeadP0 + expectedDeadP1, dead.size());

        // 投递计数：普通消息恰好投递 1 次；毒消息投递 maxRetries+1=2 次（串行重投，绝无并发重复投递）
        Set<String> deadIds = ConcurrentHashMap.newKeySet();
        dead.forEach(d -> deadIds.add(d.message().messageId()));
        deliveriesById.forEach((id, times) -> {
            Message m = dead.stream().filter(d -> d.message().messageId().equals(id)).findFirst()
                    .map(DeadLetter::message).orElse(null);
            if (m != null) {
                assertEquals(2, times, "毒消息严格串行重投：key=" + m.key() + " offset=" + m.offset());
            } else {
                assertEquals(1, times, "成功消息恰好投递一次：messageId=" + id);
            }
        });
        log.info("[并发投递结论] 不同消息投递记录数={}，成功消息各1次、死信消息各2次，无并发重复投递",
                deliveriesById.size());

        // 另一消费者组位点完全独立
        log.info("[组隔离结论] other-group 分区0 committed={}（未消费，保持 -1）",
                queue.committedOffset("other-group", TOPIC, 0));
        assertEquals(-1, queue.committedOffset("other-group", TOPIC, 0));
        assertTrue(queue.deadLetters("other-group").isEmpty());
    }

}
