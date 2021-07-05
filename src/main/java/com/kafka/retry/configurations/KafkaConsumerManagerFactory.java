package com.kafka.retry.configurations;

import com.kafka.retry.configurations.managers.KafkaConsumerManager;
import com.kafka.retry.models.KafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.util.List;

import static java.util.Arrays.asList;

@Component
@DependsOn("kafkaTopicManagerFactory")
@ConfigurationProperties(prefix = "topics.retry")
public class KafkaConsumerManagerFactory {
    @Value("${topics.retry.first-retry-topic}")
    private String firstRetryTopic;
    @Value("${topics.retry.second-retry-topic}")
    private String secondRetryTopic;
    @Value("${topics.retry.third-retry-topic}")
    private String thirdRetryTopic;
    @Value("${topics.retry.first-retry-id}")
    private String firstRetryId;
    @Value("${topics.retry.second-retry-id}")
    private String secondRetryId;
    @Value("${topics.retry.third-retry-id}")
    private String thirdRetryId;
    @Value("${topics.retry.first-retry-delay}")
    private long firstRetryDelay;
    @Value("${topics.retry.second-retry-delay}")
    private long secondRetryDelay;
    @Value("${topics.retry.third-retry-delay}")
    private long thirdRetryDelay;

    @Bean
    public KafkaConsumerManager create(KafkaListenerEndpointRegistry registry, KafkaTopicHolder kafkaTopicHolder) {
        KafkaConsumerManager kafkaConsumerManager = new KafkaConsumerManager(registry);

        List<String> topics = asList(firstRetryTopic, secondRetryTopic, thirdRetryId);
        List<String> ids = asList(firstRetryId, secondRetryId, thirdRetryId);
        List<Long> delays = asList(firstRetryDelay, secondRetryDelay, thirdRetryDelay);

        topics.stream()
                .forEach(topicName -> {
                    if (kafkaTopicHolder.getKafkaTopicByName(topicName).getType() != KafkaTopic.Type.MAIN) {
                        kafkaConsumerManager
                                .register(kafkaTopicHolder.getKafkaTopicByName(topicName),
                                    ids.get(topics.indexOf(topicName)),
                                    delays.get(topics.indexOf(topicName)));
                    }
                });

        return kafkaConsumerManager;
    }
}
