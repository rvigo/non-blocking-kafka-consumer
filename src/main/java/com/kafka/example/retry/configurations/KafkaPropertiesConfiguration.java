package com.kafka.example.retry.configurations;

import com.kafka.example.retry.properties.KafkaPropertiesProvider;
import com.kafka.example.retry.properties.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@AllArgsConstructor
public class KafkaPropertiesConfiguration {
    private final KafkaPropertiesProvider provider;

    @Bean(name = "properties")
    public Properties getProperties() {
        return Properties.builder()
                .withMaxRetries(provider.getMaxRetries())
                .withConsumers(provider.getConsumers())
                .build();
    }
}
