package com.kafka.example.retry.configurations;

import com.kafka.example.retry.managers.KafkaTopicChain;
import com.kafka.example.retry.properties.Properties;
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
    public KafkaTopicChain configureKafkaTopicHolder() {
        KafkaTopicChain topicHolder = new KafkaTopicChain();
        kafkaProperties.getConsumers()
                .stream()
                .flatMap(c -> c.getTopics().stream())
                .forEach(topicHolder::add);

        return topicHolder;
    }
}
