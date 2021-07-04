package com.kafka.retry.configurations;

import com.kafka.retry.configurations.managers.KafkaTopicManager;
import com.kafka.retry.models.KafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicManagerConfiguration {
    @Value("${topics.retry.dlq}")
    private String dlqTopic;
    @Value("${topics.retry.first-retry-topic}")
    private String firstRetryTopic;
    @Value("${topics.retry.second-retry-topic}")
    private String secondRetryTopic;
    @Value("${topics.retry.third-retry-topic}")
    private String thirdRetryTopic;

    @Bean
    public KafkaTopicManager kafkaTopicManager() {
        KafkaTopicManager manager = new KafkaTopicManager();
        KafkaTopic first = new KafkaTopic(firstRetryTopic, 1);
        KafkaTopic second = new KafkaTopic(secondRetryTopic, 2);
        KafkaTopic third = new KafkaTopic(thirdRetryTopic, 3);
        KafkaTopic dlq = new KafkaTopic(dlqTopic, 99);
        manager.add(first);
        manager.add(second);
        manager.add(third);
        manager.add(dlq);

        return manager;
    }
}
