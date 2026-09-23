package com.github.xingjianluo.localmessagequeuejava.mq;

/**
 * 重试耗尽后进入死信队列的记录。
 */
public record DeadLetter(Message message, String group, int attempts, String lastError, long deadAt) {
}
