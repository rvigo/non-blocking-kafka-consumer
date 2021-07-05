package com.kafka.retry.consumers;

import com.kafka.retry.configurations.managers.KafkaConsumerManager;
import com.kafka.retry.dtos.MessageDTO;
import com.kafka.retry.services.ExampleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import static java.lang.String.format;

@Slf4j
@Service
public class ExampleConsumer extends AbstractConsumerSeekAware {
    private static final String LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER = "LAST_RETRY_ATTEMPT_TIMESTAMP";
    private final KafkaConsumerManager kafkaConsumerManager;
    private final ExampleService service;
    @Value("${topics.retry.first-retry-topic}")
    private String firstRetryTopic;
    @Value("${topics.retry.second-retry-topic}")
    private String secondRetryTopic;
    @Value("${topics.retry.third-retry-topic}")
    private String thirdRetryTopic;


    @Autowired
    public ExampleConsumer(KafkaConsumerManager kafkaConsumerManager, ExampleService service) {
        this.kafkaConsumerManager = kafkaConsumerManager;
        this.service = service;
    }

    @KafkaListener(id = "${topics.main.id}",
            topics = "${topics.main.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, MessageDTO> message,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        log.info(format("processing message with id %s from topic %s", key, topic));
        service.process(message.value());
    }

    @KafkaListener(id = "${topics.retry.first-retry-id}",
            topics = "${topics.retry.first-retry-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaRetryListenerContainerFactory")
    public void firstRetry(ConsumerRecord<String, MessageDTO> message,
                           Acknowledgment acknowledgment,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                           @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                           @Header(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER) String timestamp) {
        long attemptTimestamp = Long.parseLong(timestamp);
        if (kafkaConsumerManager.shouldConsume(topic, attemptTimestamp)) {
            try {
                log.info(format("trying to reprocess message with id %s from topic %s-%s", key, topic, message.partition()));
                service.process(message.value());
            } finally {
                acknowledgment.acknowledge();
            }
        } else {
            kafkaConsumerManager.sleep(
                    topic,
                    message.partition(),
                    attemptTimestamp);

            //rewind offset to seek last message at given partition
            rewindOnePartitionOneRecord(firstRetryTopic, message.partition());
        }
    }

    @KafkaListener(id = "${topics.retry.second-retry-id}",
            topics = "${topics.retry.second-retry-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaRetryListenerContainerFactory")
    public void secondRetry(ConsumerRecord<String, MessageDTO> message,
                            Acknowledgment acknowledgment,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                            @Header(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER) String timestamp) {
        long attemptTimestamp = Long.parseLong(timestamp);
        if (kafkaConsumerManager.shouldConsume(topic, attemptTimestamp)) {
            try {
                log.info(format("trying to reprocess message with id %s from topic %s-%s", key, topic, message.partition()));
                service.process(message.value());
            } finally {
                acknowledgment.acknowledge();
            }
        } else {
            kafkaConsumerManager.sleep(secondRetryTopic,
                    message.partition(),
                    attemptTimestamp);

            //rewind offset to seek last message at given partition
            rewindOnePartitionOneRecord(secondRetryTopic, message.partition());
        }
    }

    @KafkaListener(id = "${topics.retry.third-retry-id}",
            topics = "${topics.retry.third-retry-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaRetryListenerContainerFactory")
    public void thirdRetry(ConsumerRecord<String, MessageDTO> message,
                           Acknowledgment acknowledgment,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                           @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                           @Header(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER) String timestamp) {
        long attemptTimestamp = Long.parseLong(timestamp);
        if (kafkaConsumerManager.shouldConsume(topic, attemptTimestamp)) {
            try {
                log.info(format("trying to reprocess message with id %s from topic %s-%s", key, topic, message.partition()));
                service.process(message.value());
            } finally {
                acknowledgment.acknowledge();
            }
        } else {
            kafkaConsumerManager.sleep(thirdRetryTopic,
                    message.partition(),
                    attemptTimestamp);

            //rewind offset to seek last message at given partition
            rewindOnePartitionOneRecord(thirdRetryTopic, message.partition());
        }
    }

    private void rewindOnePartitionOneRecord(String topic, int partition) {
        getSeekCallbackFor(new TopicPartition(topic, partition))
                .seekRelative(topic, partition, -1, true);
    }
}
