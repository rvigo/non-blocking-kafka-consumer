package com.kafka.retry.configurations.kafka.configs;

import com.kafka.retry.configurations.kafka.managers.KafkaConsumerManager;
import com.kafka.retry.configurations.kafka.managers.KafkaTopicHolder;
import com.kafka.retry.configurations.kafka.utils.Properties;
import com.kafka.retry.models.KafkaTopic;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.List;
import java.util.stream.Collectors;

@Configuration
@DependsOn("topic-holder")
@AllArgsConstructor
public class KafkaConsumerManagerConfiguration {
    private final Properties kafkaProperties;
    private final KafkaTopicHolder kafkaTopicHolder;

    @Bean
    public KafkaConsumerManager configureKafkaConsumerManager(KafkaListenerEndpointRegistry registry) {
        KafkaConsumerManager kafkaConsumerManager = new KafkaConsumerManager(registry);
        List<String> ids = kafkaProperties.getConsumerIds();
        List<Long> delays = kafkaProperties.getDelays();
        List<KafkaTopic> topicList = kafkaTopicHolder
                .stream()
                .collect(Collectors.toList());
        topicList
                .forEach(topic -> {
                    int currentIndex = topicList.indexOf(topic);
                    kafkaConsumerManager.register(topic,
                            ids.get(currentIndex),
                            delays.get(currentIndex));
                });
        return kafkaConsumerManager;
    }
}
