package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 位点提交被拒绝时的可区分原因。
 */
public enum CommitRejectionReason {
    /** 提交的位点小于等于已确认位点（回退） */
    OFFSET_REGRESSION,
    /** 提交的位点超出分区当前最大位点（越界） */
    OFFSET_OUT_OF_RANGE,
    /** 消费者组不存在 / 位点不属于该组 */
    UNKNOWN_GROUP,
    /** 主题或分区不存在 */
    UNKNOWN_PARTITION
}
