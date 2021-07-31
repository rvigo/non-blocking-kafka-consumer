package com.kafka.example.retry.configurations;

import com.kafka.example.retry.managers.KafkaTopicHolder;
import com.kafka.example.retry.resolvers.KafkaHeadersResolver;
import com.kafka.example.retry.resolvers.KafkaTopicDestinationResolver;
import com.kafka.example.dtos.MessageDTO;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import static java.util.Collections.singletonMap;
import static org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE;

@Configuration
@EnableKafka
@AllArgsConstructor
public class KafkaConsumerConfiguration {
    private static final FixedBackOff NO_LOCAL_RETRY = new FixedBackOff(0, 0);

    private final KafkaHeadersResolver headersResolver;
    private final KafkaTopicDestinationResolver kafkaTopicDestinationResolver;
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageDTO> kafkaListenerContainerFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory,
                                                                                                     KafkaTemplate<String, MessageDTO> kafkaTemplate,
                                                                                                     KafkaTopicHolder kafkaTopicHolder) {
        ConcurrentKafkaListenerContainerFactory<String, MessageDTO> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(singletonMap(Object.class, kafkaTemplate),
                kafkaTopicDestinationResolver.resolveMainTopicDestination());
        deadLetterPublishingRecoverer.setHeadersFunction(headersResolver.resolveKafkaHeaders());
        SeekToCurrentErrorHandler seekToCurrentErrorHandler = new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, NO_LOCAL_RETRY);
        factory.setErrorHandler(seekToCurrentErrorHandler);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageDTO> kafkaRetryListenerContainerFactory(ConsumerFactory<Object, Object> kafkaConsumerFactory,
                                                                                                          KafkaTemplate<String, MessageDTO> kafkaTemplate,
                                                                                                          KafkaTopicHolder kafkaTopicHolder) {
        ConcurrentKafkaListenerContainerFactory<String, MessageDTO> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(singletonMap(Object.class, kafkaTemplate),
                kafkaTopicDestinationResolver.resolveRetryTopicDestination());
        deadLetterPublishingRecoverer.setHeadersFunction(headersResolver.resolveKafkaHeaders());
        SeekToCurrentErrorHandler seekToCurrentErrorHandler = new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, NO_LOCAL_RETRY);
        factory.setErrorHandler(seekToCurrentErrorHandler);
        factory.setStatefulRetry(true);
        factory.setMissingTopicsFatal(true);
        factory.getContainerProperties().setAckMode(MANUAL_IMMEDIATE);

        return factory;
    }
}