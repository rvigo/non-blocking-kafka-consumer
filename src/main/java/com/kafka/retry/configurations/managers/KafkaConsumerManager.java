package com.kafka.retry.configurations.managers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumerManager {
    private final KafkaListenerEndpointRegistry registry;

    @Autowired
    public KafkaConsumerManager(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void pausePartition(String listenerId, String topic, int partition) {
        try {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            registry.getListenerContainer(listenerId).pausePartition(topicPartition);
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
            throw new RuntimeException(e.getLocalizedMessage());
        }
    }

    public void resumePartition(String listenerId, String topic, int partition) {
        try {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            if (registry.getListenerContainer(listenerId).isPartitionPauseRequested(topicPartition)) {
                registry.getListenerContainer(listenerId).resumePartition(topicPartition);
            }
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
            throw new RuntimeException(e.getLocalizedMessage());
        }
    }
}

