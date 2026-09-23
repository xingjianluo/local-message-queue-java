package com.github.xingjianluo.localmessagequeuejava.mq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * 进程内本地消息队列：用内存状态模拟主题 / 分区 / 消费者组。
 *
 * 投递与确认语义：
 * - 每个分区一条只追加日志，位点 offset 从 0 起连续递增，同分区投递顺序稳定。
 * - 消费者组在每个分区上维护单调不减的已确认位点；位点语义为“已处理到该消息”，
 *   即 0..committed 均已终结（CONFIRMED 或 DEAD）。
 * - poll 只返回尚未终结、且未被其他 poll 占用（IN_FLIGHT）的消息；
 *   RETRY_WAIT 中的消息在退避到期后才可再次投递，保证同一条消息不会被重复投递。
 * - commit 拒绝：UNKNOWN_GROUP（组未注册到该主题）、OFFSET_OUT_OF_RANGE（位点越界）、
 *   OFFSET_REGRESSION（位点回退）；相等位点的重复提交幂等放行，不改变状态。
 * - nack 达到重试上限后消息进入 DEAD（死信），DEAD 与 CONFIRMED 同为终结态，
 *   因此单条消息失败不会阻塞其后续消息的位点推进。
 * - 所有状态变更在同一把锁内完成，并发发布 / 并发消费下位点、去重、死信结论一致。
 */
public class LocalMessageQueue {

    private static final long DEFAULT_BACKOFF_BASE_MILLIS = 20L;

    private final int maxRetries;
    private final long backoffBaseMillis;
    private final LongSupplier clockMillis;
    private final ReentrantLock lock = new ReentrantLock();

    /** topic -> 分区数 */
    private final Map<String, Integer> topics = new HashMap<>();
    /** topic:partition -> 只追加日志 */
    private final Map<String, List<Message>> logs = new HashMap<>();
    /** topic -> (dedupKey -> 首次发布的消息) */
    private final Map<String, Map<String, Message>> dedupIndex = new HashMap<>();
    /** group -> 组状态 */
    private final Map<String, GroupState> groups = new HashMap<>();

    public LocalMessageQueue(int maxRetries) {
        this(maxRetries, DEFAULT_BACKOFF_BASE_MILLIS, System::currentTimeMillis);
    }

    public LocalMessageQueue(int maxRetries, long backoffBaseMillis, LongSupplier clockMillis) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        this.maxRetries = maxRetries;
        this.backoffBaseMillis = backoffBaseMillis;
        this.clockMillis = clockMillis;
    }

    public void createTopic(String topic, int partitions) {
        lock.lock();
        try {
            if (partitions <= 0) {
                throw new IllegalArgumentException("partitions must be > 0");
            }
            Integer existing = topics.get(topic);
            if (existing != null) {
                throw new IllegalStateException("topic already exists: " + topic);
            }
            topics.put(topic, partitions);
            for (int p = 0; p < partitions; p++) {
                logs.put(keyOf(topic, p), new ArrayList<>());
            }
            dedupIndex.put(topic, new HashMap<>());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 幂等发布：dedupKey 非空时，同一 topic 下相同 dedupKey 只有首次生效，
     * 重复发布返回首次消息（DUPLICATE），不产生新位点。
     */
    public PublishResult publish(String topic, int partition, String key, String dedupKey, String payload) {
        lock.lock();
        try {
            Integer partitionCount = topics.get(topic);
            if (partitionCount == null) {
                throw new IllegalArgumentException("unknown topic: " + topic);
            }
            if (partition < 0 || partition >= partitionCount) {
                throw new IllegalArgumentException("partition out of range: " + partition);
            }
            Map<String, Message> topicDedup = dedupIndex.get(topic);
            if (dedupKey != null) {
                Message first = topicDedup.get(dedupKey);
                if (first != null) {
                    return PublishResult.duplicate(first);
                }
            }
            List<Message> log = logs.get(keyOf(topic, partition));
            long offset = log.size();
            Message message = new Message(
                    UUID.randomUUID().toString(), topic, partition, offset,
                    key, dedupKey, payload, clockMillis.getAsLong());
            log.add(message);
            if (dedupKey != null) {
                topicDedup.put(dedupKey, message);
            }
            return PublishResult.published(message);
        } finally {
            lock.unlock();
        }
    }

    /** 注册消费者组到主题，初始位点 -1（尚无任何终结消息）。 */
    public void registerGroup(String group, String topic) {
        lock.lock();
        try {
            if (!topics.containsKey(topic)) {
                throw new IllegalArgumentException("unknown topic: " + topic);
            }
            groups.computeIfAbsent(group, g -> new GroupState())
                    .subscriptions.add(topic);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 拉取消息：从已确认位点之后开始，跳过 IN_FLIGHT（已投递未裁决）与
     * 退避未到期的 RETRY_WAIT 消息，返回的消息当场标记为 IN_FLIGHT。
     */
    public List<Message> poll(String group, String topic, int partition, int maxMessages) {
        lock.lock();
        try {
            GroupState state = requireSubscription(group, topic);
            List<Message> log = requireLog(topic, partition);
            String dpKey = keyOf(topic, partition);
            long committed = state.committed.getOrDefault(dpKey, -1L);
            long now = clockMillis.getAsLong();
            List<Message> delivered = new ArrayList<>();
            Map<Long, Attempt> perOffset = state.attempts.computeIfAbsent(dpKey, k -> new LinkedHashMap<>());
            for (long offset = committed + 1; offset < log.size() && delivered.size() < maxMessages; offset++) {
                Attempt attempt = perOffset.get(offset);
                if (attempt == null) {
                    attempt = new Attempt();
                    perOffset.put(offset, attempt);
                }
                if (attempt.phase == Phase.CONFIRMED || attempt.phase == Phase.DEAD) {
                    // 已终结消息（水位可能因前序消息未裁决而滞后）绝不再投递
                    continue;
                }
                if (attempt.phase == Phase.IN_FLIGHT) {
                    continue;
                }
                if (attempt.phase == Phase.RETRY_WAIT && attempt.nextAttemptAt > now) {
                    continue;
                }
                attempt.phase = Phase.IN_FLIGHT;
                attempt.lastPollAt = now;
                delivered.add(log.get((int) offset));
            }
            return delivered;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交位点（确认 offset 对应消息处理成功）。
     * 提交后重算连续已终结位点作为组的已确认位点。
     */
    public CommitResult commit(String group, String topic, int partition, long offset) {
        lock.lock();
        try {
            GroupState state = groups.get(group);
            if (state == null || !state.subscriptions.contains(topic)) {
                return CommitResult.reject(CommitResult.Reason.UNKNOWN_GROUP, -1L);
            }
            List<Message> log = logs.get(keyOf(topic, partition));
            if (log == null) {
                return CommitResult.reject(CommitResult.Reason.OFFSET_OUT_OF_RANGE,
                        state.committed.getOrDefault(keyOf(topic, partition), -1L));
            }
            String dpKey = keyOf(topic, partition);
            long committed = state.committed.getOrDefault(dpKey, -1L);
            if (offset < 0 || offset >= log.size()) {
                return CommitResult.reject(CommitResult.Reason.OFFSET_OUT_OF_RANGE, committed);
            }
            if (offset < committed) {
                return CommitResult.reject(CommitResult.Reason.OFFSET_REGRESSION, committed);
            }
            if (offset > committed) {
                Map<Long, Attempt> perOffset = state.attempts.computeIfAbsent(dpKey, k -> new LinkedHashMap<>());
                Attempt attempt = perOffset.computeIfAbsent(offset, o -> new Attempt());
                attempt.phase = Phase.CONFIRMED;
                advanceCommitted(state, dpKey, log);
            }
            // offset == committed：重复提交，幂等放行
            return CommitResult.ok(state.committed.getOrDefault(dpKey, -1L));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 否定应答：消息处理失败。未达重试上限进入 RETRY_WAIT（指数退避），
     * 达到上限进入 DEAD 并写入死信队列；终结后重算已确认位点。
     */
    public void nack(String group, String topic, int partition, long offset, String error) {
        lock.lock();
        try {
            GroupState state = requireSubscription(group, topic);
            List<Message> log = requireLog(topic, partition);
            String dpKey = keyOf(topic, partition);
            if (offset < 0 || offset >= log.size()) {
                throw new IllegalArgumentException("offset out of range: " + offset);
            }
            Map<Long, Attempt> perOffset = state.attempts.get(dpKey);
            Attempt attempt = perOffset.get(offset);
            if (attempt == null || attempt.phase != Phase.IN_FLIGHT) {
                throw new IllegalStateException("message is not in flight: offset=" + offset);
            }
            attempt.attempts++;
            attempt.lastError = error;
            Message message = log.get((int) offset);
            if (attempt.attempts > maxRetries) {
                attempt.phase = Phase.DEAD;
                state.deadLetters.add(new DeadLetter(message, group, attempt.attempts, error,
                        clockMillis.getAsLong()));
                advanceCommitted(state, dpKey, log);
            } else {
                attempt.phase = Phase.RETRY_WAIT;
                attempt.nextAttemptAt = clockMillis.getAsLong()
                        + backoffBaseMillis * (1L << (attempt.attempts - 1));
            }
        } finally {
            lock.unlock();
        }
    }

    /** 分区当前日志条数（下一条消息的位点）。 */
    public long logSize(String topic, int partition) {
        lock.lock();
        try {
            return requireLog(topic, partition).size();
        } finally {
            lock.unlock();
        }
    }

    public long committedOffset(String group, String topic, int partition) {
        lock.lock();
        try {
            GroupState state = groups.get(group);
            if (state == null) {
                return -1L;
            }
            return state.committed.getOrDefault(keyOf(topic, partition), -1L);
        } finally {
            lock.unlock();
        }
    }

    public List<DeadLetter> deadLetters(String group) {
        lock.lock();
        try {
            GroupState state = groups.get(group);
            if (state == null) {
                return List.of();
            }
            return List.copyOf(state.deadLetters);
        } finally {
            lock.unlock();
        }
    }

    public Optional<DeadLetter> findDeadLetter(String group, String messageId) {
        lock.lock();
        try {
            GroupState state = groups.get(group);
            if (state == null) {
                return Optional.empty();
            }
            return state.deadLetters.stream()
                    .filter(d -> d.message().messageId().equals(messageId))
                    .findFirst();
        } finally {
            lock.unlock();
        }
    }

    // ---- internals ------------------------------------------------------

    /** 从当前已确认位点的下一个开始，连续跨过 CONFIRMED / DEAD 推进位点。 */
    private void advanceCommitted(GroupState state, String dpKey, List<Message> log) {
        long committed = state.committed.getOrDefault(dpKey, -1L);
        Map<Long, Attempt> perOffset = state.attempts.get(dpKey);
        long cursor = committed;
        while (cursor + 1 < log.size()) {
            Attempt next = perOffset.get(cursor + 1);
            if (next == null || (next.phase != Phase.CONFIRMED && next.phase != Phase.DEAD)) {
                break;
            }
            cursor++;
        }
        state.committed.put(dpKey, cursor);
    }

    private GroupState requireSubscription(String group, String topic) {
        GroupState state = groups.get(group);
        if (state == null || !state.subscriptions.contains(topic)) {
            throw new IllegalArgumentException("group not subscribed to topic: " + group + " / " + topic);
        }
        return state;
    }

    private List<Message> requireLog(String topic, int partition) {
        Integer partitionCount = topics.get(topic);
        if (partitionCount == null) {
            throw new IllegalArgumentException("unknown topic: " + topic);
        }
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("partition out of range: " + partition);
        }
        return logs.get(keyOf(topic, partition));
    }

    private static String keyOf(String topic, int partition) {
        return topic + "#" + partition;
    }

    private enum Phase {
        READY, IN_FLIGHT, RETRY_WAIT, CONFIRMED, DEAD
    }

    private static final class Attempt {
        Phase phase = Phase.READY;
        int attempts;
        long nextAttemptAt;
        long lastPollAt;
        String lastError;
    }

    private static final class GroupState {
        final java.util.Set<String> subscriptions = new java.util.HashSet<>();
        /** topic#partition -> 已确认位点 */
        final Map<String, Long> committed = new HashMap<>();
        /** topic#partition -> (offset -> 投递/裁决状态) */
        final Map<String, Map<Long, Attempt>> attempts = new HashMap<>();
        final List<DeadLetter> deadLetters = new ArrayList<>();
    }
}
