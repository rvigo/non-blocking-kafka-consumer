package com.kafka.example.consumers;

import com.kafka.example.retry.managers.KafkaConsumerManager;
import com.kafka.example.dtos.MessageDTO;
import com.kafka.example.services.ExampleService;
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

import static com.kafka.example.retry.utils.KafkaRetryHeaders.LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Slf4j
@AllArgsConstructor
@Service
public class ExampleConsumer extends AbstractConsumerSeekAware {
    private final KafkaConsumerManager kafkaConsumerManager;
    private final ExampleService service;

    @KafkaListener(id = "${kafka-retry.consumers[0].id}",
            topics = "${kafka-retry.topic-base-name}",
            groupId = "${kafka-retry.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, MessageDTO> record,
                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        log.info(format("processing record with id %s from topic %s", key, record.topic()));
        service.process(record.value());
    }

    @KafkaListener(id = "${kafka-retry.consumers[1].id}",
            topics = "${kafka-retry.consumers[1].topics[0].topic-name}",
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

    @KafkaListener(id = "${kafka-retry.consumers[2].id}",
            topics = "${kafka-retry.consumers[2].topics[0].topic-name}",
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

    @KafkaListener(id = "${kafka-retry.consumers[3].id}",
            topics = "${kafka-retry.consumers[3].topics[0].topic-name}",
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
