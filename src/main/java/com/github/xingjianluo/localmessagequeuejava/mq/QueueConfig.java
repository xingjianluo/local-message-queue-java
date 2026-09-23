package com.github.xingjianluo.localmessagequeuejava.mq;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 将进程内本地消息队列注册为 Spring Bean。
 */
@Configuration
public class QueueConfig {

    @Bean
    public LocalMessageQueue localMessageQueue(
            @Value("${localmq.max-retries:3}") int maxRetries,
            @Value("${localmq.backoff-base-millis:100}") long backoffBaseMillis) {
        return new LocalMessageQueue(maxRetries, backoffBaseMillis, System::currentTimeMillis);
    }
}
