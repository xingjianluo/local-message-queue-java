package com.github.xingjianluo.localmessagequeuejava.broker;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 进程内本地消息队列：模拟主题 / 分区 / 消费者组。
 *
 * <p>语义约定：</p>
 * <ul>
 *   <li>位点 offset 从 0 开始单调递增；committed 表示「下一个待确认位点」，
 *       commit(offset) 确认该位点的消息，已确认位点只会前进不会回退。</li>
 *   <li>同一分区内消息严格按发布顺序投递；在途（已投递未确认）与退避等待中的消息不会重复投递。</li>
 *   <li>非空去重键在「主题+分区」维度幂等：并发发布只有首次生效。</li>
 *   <li>nack 触发线性退避（backoffMillis * 已投递次数），投递次数达到 maxAttempts 进入死信队列；
 *       死信位点视为已解决，不阻塞同分区后续位点推进。</li>
 * </ul>
 *
 * <p>并发边界：所有状态变更均在「分区锁」下进行，同一分区串行化，不同分区互不阻塞。</p>
 */
public class LocalMessageBroker {

    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final long DEFAULT_BACKOFF_MILLIS = 200L;

    /** topic -> 分区数 */
    private final ConcurrentMap<String, Integer> topics = new ConcurrentHashMap<>();
    /** group -> 订阅的 topic 集合 */
    private final ConcurrentMap<String, Set<String>> subscriptions = new ConcurrentHashMap<>();
    /** TopicPartition -> 分区锁 */
    private final ConcurrentMap<TopicPartition, Object> locks = new ConcurrentHashMap<>();
    /** TopicPartition -> 仅追加的消息日志（offset 即下标） */
    private final ConcurrentMap<TopicPartition, List<Message>> logs = new ConcurrentHashMap<>();
    /** TopicPartition -> dedupKey -> 首次消息位点 */
    private final ConcurrentMap<TopicPartition, ConcurrentMap<String, Long>> dedupIndexes = new ConcurrentHashMap<>();
    /** group -> TopicPartition -> 消费组分区状态 */
    private final ConcurrentMap<String, ConcurrentMap<TopicPartition, GroupPartitionState>> groupStates = new ConcurrentHashMap<>();
    /** topic -> 死信列表 */
    private final ConcurrentMap<String, List<DeadLetter>> deadLettersByTopic = new ConcurrentHashMap<>();
    /** messageId -> 死信 */
    private final ConcurrentMap<String, DeadLetter> deadLettersById = new ConcurrentHashMap<>();

    private final int maxAttempts;
    private final long backoffMillis;
    private final Clock clock;

    public LocalMessageBroker() {
        this(DEFAULT_MAX_ATTEMPTS, DEFAULT_BACKOFF_MILLIS, Clock.tickMillis(ZoneOffset.UTC));
    }

    public LocalMessageBroker(int maxAttempts, long backoffMillis, Clock clock) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts 必须 >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.backoffMillis = backoffMillis;
        this.clock = clock;
    }

    /** 创建主题（已存在则无效） */
    public void createTopic(String topic, int partitionCount) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("分区数必须 >= 1");
        }
        topics.putIfAbsent(topic, partitionCount);
    }

    /** 注册消费者组对某主题的订阅，并为每个分区初始化位点 0 */
    public void subscribe(String group, String topic) {
        requireTopic(topic);
        subscriptions.computeIfAbsent(group, g -> ConcurrentHashMap.newKeySet()).add(topic);
        int partitions = topics.get(topic);
        for (int p = 0; p < partitions; p++) {
            state(group, new TopicPartition(topic, p));
        }
    }

    /** 发布消息到指定分区，按去重键幂等 */
    public PublishResult publish(String topic, int partition, String key, String dedupKey, String payload) {
        TopicPartition tp = requirePartition(topic, partition);
        synchronized (lock(tp)) {
            List<Message> log = logs.computeIfAbsent(tp, k -> new ArrayList<>());
            if (dedupKey != null) {
                ConcurrentMap<String, Long> index =
                        dedupIndexes.computeIfAbsent(tp, k -> new ConcurrentHashMap<>());
                Long firstOffset = index.get(dedupKey);
                if (firstOffset != null) {
                    return new PublishResult(PublishOutcome.DEDUPLICATED, log.get(firstOffset.intValue()));
                }
            }
            long offset = log.size();
            Message message = new Message(
                    UUID.randomUUID().toString(), topic, partition, offset, key, dedupKey, payload, 0);
            log.add(message);
            if (dedupKey != null) {
                dedupIndexes.get(tp).put(dedupKey, offset);
            }
            return new PublishResult(PublishOutcome.ACCEPTED, message);
        }
    }

    /**
     * 拉取当前可投递消息：跳过已确认、在途以及退避未到期的消息；
     * 退避到期的消息再次投递并累加投递次数。同一分区严格按位点顺序返回。
     */
    public List<Message> poll(String group, String topic, int partition, int maxMessages) {
        if (maxMessages < 1) {
            throw new IllegalArgumentException("maxMessages 必须 >= 1");
        }
        TopicPartition tp = requirePartition(topic, partition);
        requireSubscription(group, topic);
        synchronized (lock(tp)) {
            List<Message> log = logs.get(tp);
            if (log == null || log.isEmpty()) {
                return List.of();
            }
            GroupPartitionState st = state(group, tp);
            List<Message> delivered = new ArrayList<>();
            long now = clock.millis();
            for (long offset = st.committed; offset < log.size() && delivered.size() < maxMessages; offset++) {
                if (st.ackedOutOfOrder.contains(offset)) {
                    continue;
                }
                InFlight inFlight = st.inFlight.get(offset);
                if (inFlight != null) {
                    if (now < inFlight.deliverAt) {
                        // 退避中（LEASED 哨兵或退避未到期），跳过该消息，继续看后续位点 —— 单条失败不阻塞同分区推进
                        continue;
                    }
                    // 退避到期，重新投递：nack 已把 attempts 预增为本次的投递序号，此处直接携带
                    inFlight.deliverAt = InFlight.LEASED;
                    delivered.add(withAttempts(log.get((int) offset), inFlight.attempts));
                } else {
                    InFlight fresh = new InFlight(1, InFlight.LEASED);
                    st.inFlight.put(offset, fresh);
                    delivered.add(withAttempts(log.get((int) offset), 1));
                }
            }
            return delivered;
        }
    }

    /**
     * 确认单条消息位点。允许乱序确认：已确认位点沿连续已解决位点自动推进。
     *
     * @return 成功 / 失败及可区分原因；committed 字段为该组当前已确认位点（异常时为 -1）
     */
    public CommitResult commit(String group, String topic, int partition, long offset) {
        TopicPartition tp;
        try {
            tp = requirePartition(topic, partition);
        } catch (IllegalArgumentException e) {
            return CommitResult.reject(CommitRejectionReason.UNKNOWN_PARTITION, -1L);
        }
        if (!subscriptions.containsKey(group) || !subscriptions.get(group).contains(topic)) {
            return CommitResult.reject(CommitRejectionReason.UNKNOWN_GROUP, -1L);
        }
        synchronized (lock(tp)) {
            List<Message> log = logs.getOrDefault(tp, List.of());
            GroupPartitionState st = state(group, tp);
            if (offset < st.committed) {
                return CommitResult.reject(CommitRejectionReason.OFFSET_REGRESSION, st.committed);
            }
            if (offset >= log.size()) {
                return CommitResult.reject(CommitRejectionReason.OFFSET_OUT_OF_RANGE, st.committed);
            }
            st.inFlight.remove(offset);
            st.ackedOutOfOrder.add(offset);
            advanceCommitted(st);
            return CommitResult.ok(st.committed);
        }
    }

    /**
     * 消费失败通知。仍在途的消息投递次数 +1：
     * 未达上限则进入线性退避（poll 到期才会再次投递）；达到上限转入死信队列并推进位点。
     *
     * @return 该消息最新状态快照；对已解决（已确认/已死信）位点的 nack 为空操作
     */
    public Message nack(String group, String topic, int partition, long offset) {
        TopicPartition tp = requirePartition(topic, partition);
        requireSubscription(group, topic);
        synchronized (lock(tp)) {
            List<Message> log = logs.get(tp);
            if (log == null || offset < 0 || offset >= log.size()) {
                throw new IllegalArgumentException("非法位点: " + offset);
            }
            Message original = log.get((int) offset);
            GroupPartitionState st = state(group, tp);
            if (offset < st.committed || st.ackedOutOfOrder.contains(offset)) {
                return withAttempts(original, st.lastAttempts.getOrDefault(offset, 0));
            }
            InFlight inFlight = st.inFlight.get(offset);
            int attempts = inFlight == null ? 1 : inFlight.attempts + 1;
            if (attempts >= maxAttempts) {
                st.inFlight.remove(offset);
                st.lastAttempts.put(offset, attempts);
                st.ackedOutOfOrder.add(offset);
                Message dead = withAttempts(original, attempts);
                DeadLetter dl = new DeadLetter(dead,
                        "重试耗尽：已投递 " + attempts + " 次，达到上限 " + maxAttempts, clock.millis());
                deadLettersByTopic.computeIfAbsent(topic, k -> new ArrayList<>()).add(dl);
                deadLettersById.put(dead.id(), dl);
                advanceCommitted(st);
                return dead;
            }
            long deliverAt = clock.millis() + backoffMillis * (attempts - 1);
            if (inFlight == null) {
                st.inFlight.put(offset, new InFlight(attempts, deliverAt));
            } else {
                inFlight.attempts = attempts;
                inFlight.deliverAt = deliverAt;
            }
            st.lastAttempts.put(offset, attempts);
            return withAttempts(original, attempts);
        }
    }

    /** 查询组在分区上的已确认位点（下一个待确认位点）；未知组返回 -1 */
    public long committedOffset(String group, String topic, int partition) {
        TopicPartition tp = requirePartition(topic, partition);
        if (!subscriptions.containsKey(group) || !subscriptions.get(group).contains(topic)) {
            return -1L;
        }
        synchronized (lock(tp)) {
            return state(group, tp).committed;
        }
    }

    /** 分区最大位点（下一条将分配的位点） */
    public long logEndOffset(String topic, int partition) {
        TopicPartition tp = requirePartition(topic, partition);
        synchronized (lock(tp)) {
            List<Message> log = logs.get(tp);
            return log == null ? 0L : log.size();
        }
    }

    /** 查询某主题的死信队列（快照） */
    public List<DeadLetter> deadLetters(String topic) {
        requireTopic(topic);
        return List.copyOf(deadLettersByTopic.getOrDefault(topic, List.of()));
    }

    /** 按消息 ID 查询死信 */
    public Optional<DeadLetter> deadLetter(String messageId) {
        return Optional.ofNullable(deadLettersById.get(messageId));
    }

    // ------------------------------------------------------------------

    private void advanceCommitted(GroupPartitionState st) {
        while (st.ackedOutOfOrder.remove(st.committed)) {
            st.committed++;
        }
    }

    private GroupPartitionState state(String group, TopicPartition tp) {
        return groupStates
                .computeIfAbsent(group, g -> new ConcurrentHashMap<>())
                .computeIfAbsent(tp, k -> new GroupPartitionState());
    }

    private Object lock(TopicPartition tp) {
        return locks.computeIfAbsent(tp, k -> new Object());
    }

    private static Message withAttempts(Message m, int attempts) {
        return new Message(m.id(), m.topic(), m.partition(), m.offset(), m.key(), m.dedupKey(),
                m.payload(), attempts);
    }

    private void requireTopic(String topic) {
        if (!topics.containsKey(topic)) {
            throw new IllegalArgumentException("主题不存在: " + topic);
        }
    }

    private TopicPartition requirePartition(String topic, int partition) {
        Integer count = topics.get(topic);
        if (count == null) {
            throw new IllegalArgumentException("主题不存在: " + topic);
        }
        if (partition < 0 || partition >= count) {
            throw new IllegalArgumentException(
                    "分区越界: topic=" + topic + ", partition=" + partition + ", 分区数=" + count);
        }
        return new TopicPartition(topic, partition);
    }

    private void requireSubscription(String group, String topic) {
        Set<String> topics = subscriptions.get(group);
        if (topics == null || !topics.contains(topic)) {
            throw new IllegalArgumentException("消费者组未订阅该主题: group=" + group + ", topic=" + topic);
        }
    }

    /** 主题分区 */
    private record TopicPartition(String topic, int partition) {
    }

    /** 消费者组在单个分区上的状态（仅在对应分区锁下访问） */
    private static final class GroupPartitionState {
        private long committed;
        private final TreeSet<Long> ackedOutOfOrder = new TreeSet<>();
        private final Map<Long, InFlight> inFlight = new java.util.HashMap<>();
        private final Map<Long, Integer> lastAttempts = new java.util.HashMap<>();
    }

    /** 在途消息状态 */
    private static final class InFlight {
        /** 哨兵值：消息已投递、正被消费者持有，未到期前任何 poll 都不得再次投递 */
        static final long LEASED = Long.MAX_VALUE;

        private int attempts;
        private long deliverAt;

        private InFlight(int attempts, long deliverAt) {
            this.attempts = attempts;
            this.deliverAt = deliverAt;
        }
    }
}
