package com.github.xingjianluo.localmessagequeuejava.broker;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Broker Spring 装配（骨架）。
 */
@Configuration
public class BrokerConfiguration {

    @Bean
    public LocalMessageBroker localMessageBroker() {
        return new LocalMessageBroker();
    }
}
