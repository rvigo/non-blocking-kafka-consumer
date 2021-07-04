package com.kafka.retry.configurations;

import com.kafka.retry.configurations.managers.KafkaTopicManager;
import com.kafka.retry.exceptions.NonRecoverableException;
import com.kafka.retry.exceptions.RecoverableException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE;

@Slf4j
@Configuration
@EnableKafka
@DependsOn("kafkaTopicManager")
public class ConsumerConfiguration {
    private KafkaTopicManager kafkaTopicManager;
    @Value("${topics.main.topic}")
    private String mainTopic;
    @Value("${topics.retry.dlq}")
    private String dlqTopic;
    @Value("${topics.retry.max-retries}")
    private Integer numberOfRetries;

    private static final String LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER = "LAST_RETRY_ATTEMPT_TIMESTAMP";
    private static final FixedBackOff NO_RETRY = new FixedBackOff(0, 0);

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory,
                                                                                                 KafkaTemplate kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(singletonMap(Object.class, kafkaTemplate), mainResolver());
        factory.setErrorHandler(new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, NO_RETRY));

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaRetryListenerContainerFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory,
                                                                                                      KafkaTemplate kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(singletonMap(Object.class, kafkaTemplate), retryResolver());
        deadLetterPublishingRecoverer.setHeadersFunction(headersResolver());
        factory.setErrorHandler(new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, NO_RETRY));
        factory.setStatefulRetry(true);
        factory.setMissingTopicsFatal(true);
        factory.getContainerProperties().setAckMode(MANUAL_IMMEDIATE);

        return factory;
    }

    //prepare headers with current attempt timestamp
    public BiFunction<ConsumerRecord<?, ?>, Exception, Headers> headersResolver() {
        return (record, exception) -> {
            Headers headers = record.headers();
            long attemptTimestamp = currentTimeMillis();

            if (null != headers.lastHeader(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER)) {
                headers.remove(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER);
            }
            byte[] attemptTimestampInBytes = valueOf(attemptTimestamp).getBytes(UTF_8);
            headers.add(new RecordHeader(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER,
                    attemptTimestampInBytes));
            return headers;
        };
    }

    // resolve topic destination for the main consumer
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> mainResolver() {
        return (record, exception) -> {
            if (isRecoverableException(exception)) {
                String source = record.topic() != null && !record.topic().isEmpty() ? record.topic() : mainTopic;
                String target = kafkaTopicManager.getTopicByName(source).getTopicName();
                return new TopicPartition(target, record.partition());
            }
            return new TopicPartition(kafkaTopicManager.getLastTopic().getTopicName(), record.partition());
        };
    }

    //resolve topic destination for retries consumer
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> retryResolver() {
        return (record, exception) -> {
            String target;
            int actualRetryCount = kafkaTopicManager.getTopicByName(record.topic()).getRetryCount();
            if (actualRetryCount < numberOfRetries)
                target = kafkaTopicManager.getTopicByRetryCount(actualRetryCount).getNextTopic().getTopicName();
            else
                target = kafkaTopicManager.getLastTopic().getTopicName();
            return new TopicPartition(target, record.partition());
        };
    }

    private List<Class<? extends Exception>> recoverableExceptionList() {
        List<Class<? extends Exception>> exceptionList = new ArrayList<>();
        exceptionList.add(RecoverableException.class);
        exceptionList.add(NonRecoverableException.class);

        return exceptionList;
    }

    private boolean isRecoverableException(Exception exception) {
        List<Class<? extends Exception>> list = recoverableExceptionList();
        Class<? extends Throwable> clazz = exception.getCause().getClass();

        return list.contains(clazz);
    }
}