package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 发布返回值。
 *
 * @param outcome  ACCEPTED 或 DEDUPLICATED
 * @param message  ACCEPTED 时为新消息；DEDUPLICATED 时为首次发布的那条消息
 */
public record PublishResult(PublishOutcome outcome, Message message) {
}
