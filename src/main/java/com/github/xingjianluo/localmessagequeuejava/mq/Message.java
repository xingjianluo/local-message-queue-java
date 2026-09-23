package com.github.xingjianluo.localmessagequeuejava.mq;

/**
 * 分区内一条不可变消息。offset 为该消息在分区内的位点（从 0 起递增）。
 */
public record Message(String messageId,
                      String topic,
                      int partition,
                      long offset,
                      String key,
                      String dedupKey,
                      String payload,
                      long timestamp) {
}
