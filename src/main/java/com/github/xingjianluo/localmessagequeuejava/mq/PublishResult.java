package com.github.xingjianluo.localmessagequeuejava.mq;

/**
 * 发布结果：首次发布返回 PUBLISHED；同去重键重复发布返回 DUPLICATE，
 * 且 DUPLICATE 结果带回首次发布时的消息与位点，不会产生新消息。
 */
public record PublishResult(Status status, Message message) {

    public enum Status {
        PUBLISHED,
        DUPLICATE
    }

    public static PublishResult published(Message message) {
        return new PublishResult(Status.PUBLISHED, message);
    }

    public static PublishResult duplicate(Message first) {
        return new PublishResult(Status.DUPLICATE, first);
    }
}
