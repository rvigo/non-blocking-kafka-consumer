package com.kafka.example.retry.managers;

import com.kafka.example.exceptions.CustomException;
import com.kafka.example.retry.models.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

@Slf4j
public class KafkaConsumerManager {
    private final KafkaListenerEndpointRegistry registry;
    private final List<Consumer> consumers;

    @Autowired
    public KafkaConsumerManager(KafkaListenerEndpointRegistry registry, List<Consumer> consumers) {
        this.registry = registry;
        this.consumers = consumers;
    }

    @SneakyThrows
    public void sleep(String topic, int partition, long attemptTimestamp) {
        long delay = getBackOffDelayByTopic(topic);
        pausePartition(topic, partition);
        long sleep = delay - (currentTimeMillis() - attemptTimestamp);
        log.debug(format("sleeping partition consumption for %s ms", sleep));
        Thread.sleep(sleep);
        resumePartition(topic, partition);
    }

    private long getBackOffDelayByTopic(String topic) {
        Consumer consumer = consumers
                .stream()
                .filter(t -> t.getTopics()
                        .stream()
                        .anyMatch(q -> q.getTopicName().equals(topic)))
                .findFirst()
                .orElseThrow();
        return consumer.getDelay();
    }

    public boolean shouldConsume(String topic, long attemptTimestamp) {
        return currentTimeMillis() >= (attemptTimestamp + getBackOffDelayByTopic(topic));
    }

    private void pausePartition(String topic, int partition) {
        try {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            getListenerContainer(getListenerIdByTopicName(topic)).pausePartition(topicPartition);
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
            throw new CustomException(e.getLocalizedMessage());
        }
    }

    private void resumePartition(String topic, int partition) {
        try {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            MessageListenerContainer container = getListenerContainer(getListenerIdByTopicName(topic));
            if (container.isPartitionPauseRequested(topicPartition)) {
                container.resumePartition(topicPartition);
            }
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
            throw new CustomException(e.getLocalizedMessage());
        }
    }

    private MessageListenerContainer getListenerContainer(String listenerId) {
        return registry.getListenerContainer(listenerId);
    }

    private String getListenerIdByTopicName(String topic) {
        Consumer consumer = consumers.stream()
                .filter(c -> c.getTopics()
                        .stream()
                        .anyMatch(t -> t.getTopicName().equals(topic)))
                .findFirst()
                .orElseThrow();

        return consumer.getId();
    }
}
