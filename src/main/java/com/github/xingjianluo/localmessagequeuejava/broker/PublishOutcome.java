package com.github.xingjianluo.localmessagequeuejava.broker;

/**
 * 发布结果：首次发布 ACCEPTED；去重键命中重复时 DEDUPLICATED。
 */
public enum PublishOutcome {
    ACCEPTED,
    DEDUPLICATED
}
