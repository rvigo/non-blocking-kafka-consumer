package com.kafka.retry.configurations;

import com.kafka.retry.exceptions.NonRecoverableException;
import com.kafka.retry.exceptions.RecoverableException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static java.util.Collections.singletonMap;


@Configuration
@EnableKafka
public class ConsumerConfiguration {

    @Autowired
    private KafkaTemplate kafkaTemplate;
    static final FixedBackOff NO_RETRY = new FixedBackOff(0, 0);
    static final String ERROR_TOPIC = "my_topic_group_id_ERROR";
    static final String TOPIC_PATTERN = "my_topic_group_id_RETRY_[0-9]+";
    static final String FIRST_RETRY_TOPIC = "my_topic_group_id_RETRY_1";
    static final String TOPIC = "my_topic";
    static final Integer NUMBER_OF_RETRIES = 2;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(singletonMap(Object.class, kafkaTemplate), mainResolver());
        factory.setErrorHandler(new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, NO_RETRY));

        return factory;
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaRetryListenerContainerFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(singletonMap(Object.class, kafkaTemplate), retryResolver());
        factory.setErrorHandler(new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, NO_RETRY));

        return factory;
    }

    @Bean
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition>
    mainResolver() {
        return (r, e) -> {
            //checks if the exception is recoverable
            //if not, send the message directly to DLT topic
            if (isRecoveryException(e)) {
                String source = r.topic() != null && !r.topic().isEmpty()?r.topic():TOPIC;
                System.out.println(LocalDateTime.now() + " - " + r.key() + "- receiving message from " + source);

                String target = !source.matches(TOPIC_PATTERN) ? FIRST_RETRY_TOPIC : ERROR_TOPIC;
                System.out.println(LocalDateTime.now() + " - " + r.key() + " - sending message to topic " + target);
                return new TopicPartition(target, r.partition());
            }
            System.out.println(LocalDateTime.now() + " - " + r.key() + " - sending message to topic " + ERROR_TOPIC);
            return new TopicPartition(ERROR_TOPIC, r.partition());
        };
    }

    @Bean
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition>
    retryResolver() {
        return (r, e) -> {
            String source = r.topic().matches(TOPIC_PATTERN)?r.topic():FIRST_RETRY_TOPIC;
            System.out.println(LocalDateTime.now() + " - " + r.key() + " - receiving message from " + source);

            String target = "";
            int topic_number = Integer.parseInt(source.substring(source.lastIndexOf("_")).split("_")[1]);

            if (topic_number < NUMBER_OF_RETRIES)
                target = source.substring(0, source.lastIndexOf("_")) + "_" + (topic_number + 1);
            else
                target = ERROR_TOPIC;

            System.out.println(LocalDateTime.now() + " - " + r.key() + " - sending message to topic " + target);
            return new TopicPartition(target, r.partition());
        };
    }

    private List<Class<? extends Exception>> recoverableExceptionList() {
        List<Class<? extends Exception>> exceptionList = new ArrayList<>();
        exceptionList.add(RecoverableException.class);
        exceptionList.add(NonRecoverableException.class);

        return exceptionList;
    }

    private boolean isRecoveryException(Exception exception) {
        List<Class<? extends Exception>> list = recoverableExceptionList();
        Class clazz = exception.getCause().getClass();

        return list.contains(clazz);
    }
}