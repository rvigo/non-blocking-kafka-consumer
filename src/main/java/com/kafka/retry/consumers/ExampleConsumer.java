package com.kafka.retry.consumers;


import com.kafka.retry.configurations.managers.KafkaConsumerManager;
import com.kafka.retry.exceptions.RecoverableException;
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
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;

@Slf4j
@Service
public class ExampleConsumer extends AbstractConsumerSeekAware {
    private static final String MAIN_TOPIC_ID = "MAIN";
    private static final String FIRST_RETRY_ID = "RETRY_1";
    private static final String SECOND_RETRY_ID = "RETRY_2";
    private static final String THIRD_RETRY_ID = "RETRY_3";

    @Value("${topics.retry.first-retry-topic}")
    private String firstRetryTopic;
    @Value("${topics.retry.second-retry-topic}")
    private String secondRetryTopic;
    @Value("${topics.retry.third-retry-topic}")
    private String thirdRetryTopic;

    private static final long firstRetryDelay = 60000L;
    private static final long secondRetryDelay = 120000L;
    private static final long thirdRetryDelay = 240000L;

    private final KafkaConsumerManager kafkaConsumerManager;
    private final ExampleService service;

    @Autowired
    public ExampleConsumer(KafkaConsumerManager kafkaConsumerManager, ExampleService service) {
        this.kafkaConsumerManager = kafkaConsumerManager;
        this.service = service;
    }

    @KafkaListener(id = MAIN_TOPIC_ID,
            topics = "${topics.main.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> message,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        log.info(format("processing message with id %s from topic %s", key, topic));
        throw new RecoverableException("sending to RETRY 1");
    }

    @KafkaListener(id = FIRST_RETRY_ID,
            topics = "${topics.retry.first-retry-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaRetryListenerContainerFactory")
    public void firstRetry(ConsumerRecord<String, String> message,
                           Acknowledgment acknowledgment,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                           @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                           @Header("attempt_timestamp") String timestamp) throws InterruptedException {
        long attemptTimestamp = Long.parseLong(timestamp);
        if (shouldProcess(firstRetryDelay, attemptTimestamp)) {
            acknowledgment.acknowledge();
            log.info(format("trying to reprocess message with id %s from topic %s-%s", key, topic, message.partition()));
            throw new RecoverableException("sending to RETRY 2");
        } else {
            sleepPartitionConsumption(FIRST_RETRY_ID,
                    topic,
                    message.partition(),
                    getRemainingSleepTimeInMillis(firstRetryDelay, attemptTimestamp));

            //rewind offset to seek last message at given partition
            rewindOnePartitionOneRecord(firstRetryTopic, message.partition());
        }
    }

    @KafkaListener(id = SECOND_RETRY_ID, topics = "${topics.retry.second-retry-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaRetryListenerContainerFactory")
    public void secondRetry(ConsumerRecord<String, String> message,
                            Acknowledgment acknowledgment,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                            @Header("attempt_timestamp") String timestamp) throws InterruptedException {
        long attemptTimestamp = Long.parseLong(timestamp);
        if (shouldProcess(secondRetryDelay, attemptTimestamp)) {
            acknowledgment.acknowledge();
            log.info(format("trying to reprocess message with id %s from topic %s-%s", key, topic, message.partition()));
            throw new RecoverableException("sending to RETRY 3");
        } else {
            sleepPartitionConsumption(SECOND_RETRY_ID,
                    secondRetryTopic,
                    message.partition(),
                    getRemainingSleepTimeInMillis(secondRetryDelay, attemptTimestamp));

            //rewind offset to seek last message at given partition
            rewindOnePartitionOneRecord(secondRetryTopic, message.partition());
        }
    }

    @KafkaListener(id = THIRD_RETRY_ID,
            topics = "${topics.retry.third-retry-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaRetryListenerContainerFactory")
    public void thirdRetry(ConsumerRecord<String, String> message,
                           Acknowledgment acknowledgment,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                           @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                           @Header("attempt_timestamp") String timestamp) throws InterruptedException {
        long attemptTimestamp = Long.parseLong(timestamp);
        if (shouldProcess(thirdRetryDelay, attemptTimestamp)) {
            acknowledgment.acknowledge();
            log.info(format("message with id %s from topic %s-%s: DONE", key, topic, message.partition()));
        } else {
            sleepPartitionConsumption(THIRD_RETRY_ID,
                    thirdRetryTopic,
                    message.partition(),
                    getRemainingSleepTimeInMillis(thirdRetryDelay, attemptTimestamp));

            //rewind offset to seek last message at given partition
            rewindOnePartitionOneRecord(thirdRetryTopic, message.partition());
        }
    }

    private void sleepPartitionConsumption(String topicId, String topic, int partition, long timeToSleep) throws InterruptedException {
        kafkaConsumerManager.pausePartition(topicId, topic, partition);
        sleep(timeToSleep);
        kafkaConsumerManager.resumePartition(topicId, topic, partition);
    }

    private long getRemainingSleepTimeInMillis(long delay, long attemptTimestamp) {
        return delay - (currentTimeMillis() - attemptTimestamp);
    }

    private void rewindOnePartitionOneRecord(String topic, int partition) {
        getSeekCallbackFor(new TopicPartition(topic, partition))
                .seekRelative(topic, partition, -1, true);
    }

    private boolean shouldProcess(long delay, long timestamp) {
        return System.currentTimeMillis() >= timestamp + delay;
    }
}
