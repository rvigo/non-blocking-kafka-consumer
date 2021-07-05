package com.kafka.retry.configurations.managers;

import com.kafka.retry.exceptions.CustomException;
import com.kafka.retry.models.KafkaTopic;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singletonList;

@Slf4j
@Component
public class KafkaConsumerManager {
    private final KafkaListenerEndpointRegistry registry;
    // key: listenerId (String), value: List<KafkaConsumerHolder>>
    private final Map<String, List<KafkaConsumerHolder>> kafkaConsumerHolderMap;

    @Autowired
    public KafkaConsumerManager(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
        this.kafkaConsumerHolderMap = new HashMap<>();
    }

    public void register(@NotNull KafkaTopic kafkaTopic, @NotNull String listenerId, long delay) {
        KafkaConsumerHolder holder = KafkaConsumerHolder.builder()
                .withKafkaTopicList(singletonList(kafkaTopic))
                .withBackOffDelay(delay)
                .build();
        log.info(String.format("registering topic %s", kafkaTopic.getTopicName()));
        if (null == kafkaConsumerHolderMap.get(listenerId)) {
            kafkaConsumerHolderMap.put(listenerId, singletonList(holder));
        } else {
            kafkaConsumerHolderMap.get(listenerId).add(holder);
        }
    }

    public void unregister(@NotNull KafkaTopic kafkaTopic, @NotNull String listenerId) {
        if (null != kafkaConsumerHolderMap.get(listenerId)) {
            kafkaConsumerHolderMap.get(listenerId).stream()
                    .filter(ch -> ch.kafkaTopicList
                            .stream()
                            .anyMatch(k -> k.equals(kafkaTopic)))
                    .forEach(ch -> ch.kafkaTopicList.remove(kafkaTopic));
        }
    }

    @SneakyThrows
    public void sleep(String topic, int partition, long attemptTimestamp) {
        KafkaConsumerHolder holder = getHolderByTopic(topic);
        pausePartition(topic, partition);
        Thread.sleep(holder.getBackOffDelay() - (currentTimeMillis() - attemptTimestamp));
        resumePartition(topic, partition);
    }

    public boolean shouldConsume(String topic, long attemptTimestamp) {
        return currentTimeMillis() >= (attemptTimestamp + getHolderByTopic(topic).getBackOffDelay());
    }

    private KafkaConsumerHolder getHolderByTopic(String topic) {
        return kafkaConsumerHolderMap.values()
                .stream()
                .flatMap(Collection::stream)
                .filter(holder -> holder.kafkaTopicList
                        .stream()
                        .anyMatch(kafkaTopic -> kafkaTopic.getTopicName().equals(topic)))
                .findFirst()
                .orElseThrow(RuntimeException::new);
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
        return kafkaConsumerHolderMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue()
                        .stream()
                        .anyMatch(ch -> ch.kafkaTopicList
                                .stream()
                                .anyMatch(t -> t.getTopicName().equals(topic))))
                .findFirst()
                .orElseThrow(RuntimeException::new)
                .getKey();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder(setterPrefix = "with")
    private static class KafkaConsumerHolder {
        private long backOffDelay;
        private List<KafkaTopic> kafkaTopicList;
    }
}