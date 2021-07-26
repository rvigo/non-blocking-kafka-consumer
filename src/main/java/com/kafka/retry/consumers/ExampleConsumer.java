package com.kafka.retry.consumers;

import com.kafka.retry.configurations.kafka.managers.KafkaConsumerManager;
import com.kafka.retry.dtos.MessageDTO;
import com.kafka.retry.services.ExampleService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import static com.kafka.retry.configurations.kafka.utils.KafkaRetryHeaders.LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Slf4j
@AllArgsConstructor
@Service
public class ExampleConsumer extends AbstractConsumerSeekAware {
    private final KafkaConsumerManager kafkaConsumerManager;
    private final ExampleService service;

    @KafkaListener(id = "${topics.main.id}",
            topics = "${topics.main.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, MessageDTO> record,
                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        log.info(format("processing record with id %s from topic %s", key, record.topic()));
        service.process(record.value());
    }

    @KafkaListener(id = "${topics.retry.first-retry-id}",
            topics = "${topics.retry.first-retry-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaRetryListenerContainerFactory")
    public void firstRetry(ConsumerRecord<String, MessageDTO> record,
                           Acknowledgment acknowledgment,
                           @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                           @Header(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER) String timestamp) {
        long attemptTimestamp = Long.parseLong(timestamp);
        if (kafkaConsumerManager.shouldConsume(record.topic(), attemptTimestamp)) {
            try {
                log.info(format("trying to reprocess message with id %s from topic %s-%s", key, record.topic(), record.partition()));
                service.process(record.value());
            } finally {
                acknowledgment.acknowledge();
            }
        } else {
            kafkaConsumerManager.sleep(record.topic(),
                    record.partition(),
                    attemptTimestamp);

            //rewind offset to seek last message at given partition
            rewindOnePartitionOneRecord(record.topic(), record.partition());
        }
    }

    @KafkaListener(id = "${topics.retry.second-retry-id}",
            topics = "${topics.retry.second-retry-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaRetryListenerContainerFactory")
    public void secondRetry(ConsumerRecord<String, MessageDTO> record,
                            Acknowledgment acknowledgment,
                            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                            @Header(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER) String timestamp) {
        long attemptTimestamp = Long.parseLong(timestamp);
        if (kafkaConsumerManager.shouldConsume(record.topic(), attemptTimestamp)) {
            try {
                log.info(format("trying to reprocess message with id %s from topic %s-%s", key, record.topic(), record.partition()));
                service.process(record.value());
            } finally {
                acknowledgment.acknowledge();
            }
        } else {
            kafkaConsumerManager.sleep(record.topic(),
                    record.partition(),
                    attemptTimestamp);

            //rewind offset to seek last message at given partition
            rewindOnePartitionOneRecord(record.topic(), record.partition());
        }
    }

    @KafkaListener(id = "${topics.retry.third-retry-id}",
            topics = "${topics.retry.third-retry-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaRetryListenerContainerFactory")
    public void thirdRetry(ConsumerRecord<String, MessageDTO> record,
                           Acknowledgment acknowledgment,
                           @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                           @Header(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER) String timestamp) {
        long attemptTimestamp = Long.parseLong(timestamp);
        if (kafkaConsumerManager.shouldConsume(record.topic(), attemptTimestamp)) {
            try {
                log.info(format("trying to reprocess message with id %s from topic %s-%s", key, record.topic(), record.partition()));
                record.value().setOriginName("A");
                service.process(record.value());
            } finally {
                acknowledgment.acknowledge();
            }
        } else {
            kafkaConsumerManager.sleep(record.topic(),
                    record.partition(),
                    attemptTimestamp);

            //rewind offset to seek last message at given partition
            rewindOnePartitionOneRecord(record.topic(), record.partition());
        }
    }

    private void rewindOnePartitionOneRecord(String topic, int partition) {
        requireNonNull(getSeekCallbackFor(new TopicPartition(topic, partition)))
                .seekRelative(topic, partition, -1, true);
    }
}
