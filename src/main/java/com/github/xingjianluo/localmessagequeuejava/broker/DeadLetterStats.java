package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 某主题死信队列的统一统计口径。
 *
 * @param topic          主题
 * @param current        当前死信条数（与按主题列出的死信数量一致）
 * @param totalEntered   累计进入死信的条数（每进入一次计一次，清理/淘汰/重放不回减）
 * @param retentionLimit 死信保留上限；-1 表示不限
 */
public record DeadLetterStats(String topic, long current, long totalEntered, int retentionLimit) {
}
