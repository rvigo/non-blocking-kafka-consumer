package com.kafka.example.retry.configurations;

import com.kafka.example.retry.managers.KafkaTopicHolder;
import com.kafka.example.retry.utils.Properties;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
@DependsOn("properties")
@AllArgsConstructor
public class KafkaTopicHolderConfiguration {
    private final Properties kafkaProperties;

    @Bean(name = "topic-holder")
    public KafkaTopicHolder configureKafkaTopicHolder() {
        KafkaTopicHolder topicHolder = new KafkaTopicHolder();
        kafkaProperties.getConsumers()
                .stream()
                .flatMap(c -> c.getTopics().stream())
                .forEach(topicHolder::add);

        return topicHolder;
    }
}
