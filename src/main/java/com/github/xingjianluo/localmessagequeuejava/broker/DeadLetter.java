package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 重试耗尽后进入死信队列的消息快照。
 *
 * @param message        最后一次投递时的消息（attempts 为实际投递次数）
 * @param reason         进入死信的原因
 * @param deadLetteredAt 进入死信的时间戳（毫秒）
 */
public record DeadLetter(
        Message message,
        String reason,
        long deadLetteredAt
) {
}
