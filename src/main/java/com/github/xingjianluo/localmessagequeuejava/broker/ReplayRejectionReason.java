package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 死信重放拒绝原因（可区分）。
 */
public enum ReplayRejectionReason {
    /** 消息 ID 从未进入过该主题的死信队列 */
    UNKNOWN_MESSAGE,
    /** 该消息是其它主题的死信，不属于指定主题 */
    TOPIC_MISMATCH,
    /** 该死信已被清理或被淘汰，无法重放 */
    ALREADY_PURGED,
    /** 该死信已被重放过、尚未再次进入死信队列 */
    ALREADY_REPLAYED
}
