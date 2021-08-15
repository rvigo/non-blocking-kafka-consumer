package com.kafka.example.retry.configurations;

import com.kafka.example.retry.managers.KafkaConsumerManager;
import com.kafka.example.retry.properties.Properties;
import lombok.AllArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

@Configuration
@DependsOn("topic-holder")
@AllArgsConstructor
public class KafkaConsumerManagerConfiguration {
    private final Properties kafkaProperties;

    @Bean
    public KafkaConsumerManager configureKafkaConsumerManager(KafkaListenerEndpointRegistry registry) {
        return new KafkaConsumerManager(registry, kafkaProperties.getConsumers());
    }
}
