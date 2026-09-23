package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 投递到本地消息队列分区中的一条消息。
 *
 * @param id        消息唯一 ID
 * @param topic     主题
 * @param partition 分区（0 起）
 * @param offset    分区内单调递增位点
 * @param key       消息键，用于日志排查
 * @param dedupKey  去重键，可为 null；同分区内非空去重键仅首次发布生效
 * @param payload   消息体
 * @param attempts  已投递次数
 */
public record Message(
        String id,
        String topic,
        int partition,
        long offset,
        String key,
        String dedupKey,
        String payload,
        int attempts
) {
}
