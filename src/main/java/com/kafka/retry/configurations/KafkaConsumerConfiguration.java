package com.kafka.retry.configurations;

import com.kafka.retry.dtos.MessageDTO;
import com.kafka.retry.exceptions.RecoverableException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
public class KafkaConsumerConfiguration {
    private static final String LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER = "LAST_RETRY_ATTEMPT_TIMESTAMP";
    private static final String FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER = "FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER";
    private static final FixedBackOff NO_LOCAL_RETRY = new FixedBackOff(0, 0);

    @Value("${topics.main.topic}")
    private String mainTopic;
    @Value("${topics.retry.dlq}")
    private String dlqTopic;
    @Value("${topics.retry.max-retries}")
    private Integer numberOfRetries;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageDTO> kafkaListenerContainerFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory,
                                                                                                 KafkaTemplate<String, MessageDTO> kafkaTemplate, KafkaTopicHolder kafkaTopicHolder) {
        ConcurrentKafkaListenerContainerFactory<String, MessageDTO> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(singletonMap(Object.class, kafkaTemplate), mainTopicResolver(kafkaTopicHolder));
        deadLetterPublishingRecoverer.setHeadersFunction(headersResolver());
        SeekToCurrentErrorHandler seekToCurrentErrorHandler = new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, NO_LOCAL_RETRY);
        factory.setErrorHandler(seekToCurrentErrorHandler);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageDTO> kafkaRetryListenerContainerFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory,
                                                                                                      KafkaTemplate<String, MessageDTO> kafkaTemplate, KafkaTopicHolder kafkaTopicHolder) {
        ConcurrentKafkaListenerContainerFactory<String, MessageDTO> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(singletonMap(Object.class, kafkaTemplate), retryTopicResolver(kafkaTopicHolder));
        deadLetterPublishingRecoverer.setHeadersFunction(headersResolver());
        SeekToCurrentErrorHandler seekToCurrentErrorHandler = new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, NO_LOCAL_RETRY);
        factory.setErrorHandler(seekToCurrentErrorHandler);
        factory.setStatefulRetry(true);
        factory.setMissingTopicsFatal(true);
        factory.getContainerProperties().setAckMode(MANUAL_IMMEDIATE);

        return factory;
    }

    //prepare headers with current attempt timestamp
    private BiFunction<ConsumerRecord<?, ?>, Exception, Headers> headersResolver() {
        return (record, exception) -> {
            Headers headers = record.headers();
            long attemptTimestamp = currentTimeMillis();
            byte[] attemptTimestampInBytes = valueOf(attemptTimestamp).getBytes(UTF_8);

            if (null == headers.lastHeader(FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER)) {
                headers.add(new RecordHeader(FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER, attemptTimestampInBytes));
            }
            if (null != headers.lastHeader(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER)) {
                headers.remove(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER);
            }
            headers.add(new RecordHeader(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER,
                    attemptTimestampInBytes));
            return headers;
        };
    }

    // resolve topic destination for the main consumer
    private BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> mainTopicResolver(KafkaTopicHolder kafkaTopicHolder) {
        return (record, exception) -> {
            if (isRecoverableException(exception)) {
                String source = record.topic() != null && !record.topic().isEmpty() ? record.topic() : kafkaTopicHolder.getFirstTopic().getTopicName();
                String target = kafkaTopicHolder.getKafkaTopicByName(source).getNextTopic().getTopicName();
                return new TopicPartition(target, record.partition());
            }
            log.debug(String.format("sending message with id %s to dlq ", record.key()));
            return new TopicPartition(kafkaTopicHolder.getLastTopic().getTopicName(), record.partition());
        };
    }

    //resolve topic destination for retries consumer
    private BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> retryTopicResolver(KafkaTopicHolder kafkaTopicHolder) {
        return (record, exception) -> {
            String target;
            int actualRetryCount = kafkaTopicHolder.getKafkaTopicByName(record.topic()).getRetryValue();
            if (actualRetryCount < numberOfRetries) {
                target = kafkaTopicHolder.getKafkaTopicByRetryValue(actualRetryCount).getNextTopic().getTopicName();
            } else {
                log.debug(String.format("sending message %s to dlq ", record.key()));
                target = kafkaTopicHolder.getLastTopic().getTopicName();
            }
            return new TopicPartition(target, record.partition());
        };
    }

    private List<Class<? extends Exception>> recoverableExceptionList() {
        List<Class<? extends Exception>> exceptionList = new ArrayList<>();
        exceptionList.add(RecoverableException.class);
        return exceptionList;
    }

    private boolean isRecoverableException(Exception exception) {
        List<Class<? extends Exception>> list = recoverableExceptionList();
        Class<? extends Throwable> clazz = exception.getCause().getClass();

        return list.contains(clazz);
    }
}