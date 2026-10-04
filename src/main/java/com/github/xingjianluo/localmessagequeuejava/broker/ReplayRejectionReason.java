package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 死信重放被拒绝时的可区分原因。
 */
public enum ReplayRejectionReason {
    /** 该消息当前不在死信队列中（从未进入过死信） */
    DEAD_LETTER_NOT_FOUND,
    /** 该消息曾经进入死信，但已被清理或被淘汰 */
    DEAD_LETTER_PURGED,
    /** 死信存在，但不属于指定主题 */
    TOPIC_MISMATCH
}
