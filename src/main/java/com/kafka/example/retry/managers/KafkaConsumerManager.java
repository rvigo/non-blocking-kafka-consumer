package com.kafka.example.retry.managers;

import com.kafka.example.retry.entities.Consumer;
import com.kafka.example.retry.exceptions.PartitionManagementException;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

@Slf4j
@AllArgsConstructor
public class KafkaConsumerManager {
    private final KafkaListenerEndpointRegistry registry;
    private final List<Consumer> consumers;

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
            throw new PartitionManagementException("An error occurred while pausing the desired partition", e);
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
            throw new PartitionManagementException("An error occurred while resuming the desired partition", e);
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
