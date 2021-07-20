package com.kafka.retry.configurations.kafka.configs;

import com.kafka.retry.configurations.kafka.managers.KafkaTopicHolder;
import com.kafka.retry.configurations.kafka.utils.Properties;
import com.kafka.retry.models.KafkaTopic;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.util.List;

@Configuration
@DependsOn("properties")
@AllArgsConstructor
public class KafkaTopicHolderConfiguration {
    private final Properties kafkaProperties;

    @Bean(name = "topic-holder")
    public KafkaTopicHolder configureKafkaTopicHolder() {
        KafkaTopicHolder topicHolder = new KafkaTopicHolder();
        List<KafkaTopic> kafkaTopicList = kafkaProperties.getTopics();
        kafkaTopicList.forEach(topicHolder::add);

        return topicHolder;
    }
}
