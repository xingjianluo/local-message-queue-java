package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 主题死信统计口径。
 *
 * @param topic            主题
 * @param currentCount     当前死信队列中的条数（与列表查询一致）
 * @param cumulativeCount  累计进入死信队列的条数（含已重放/已清理/已淘汰，只增不减）
 * @param maxRetained      死信保留上限；null 表示不限制
 */
public record DeadLetterStats(
        String topic,
        long currentCount,
        long cumulativeCount,
        Integer maxRetained
) {
}
