package com.kafka.retry.configurations.kafka.configs;

import com.kafka.retry.configurations.kafka.utils.KafkaPropertiesProvider;
import com.kafka.retry.configurations.kafka.utils.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@Slf4j
@Configuration
@AllArgsConstructor
public class KafkaPropertiesConfiguration {
    private final KafkaPropertiesProvider provider;

    @Bean(name = "properties")
    public Properties getProperties() {
        return Properties.builder()
                .withDelays(provider.getDelays())
                .withConsumerIds(provider.getConsumerIds())
                .withMaxRetries(provider.getMaxRetries())
                .withTopicPrefix(provider.getTopicRetryPrefix())
                .withRecoverableExceptions(convertStringListToClassList(provider.getExceptions().getRecoverable()))
                .withUnrecoverableExceptions(convertStringListToClassList(provider.getExceptions().getUnrecoverable()))
                .withTopics(provider.getTopics())
                .build();
    }

    private List<Class<?>> convertStringListToClassList(List<String> pathToClassList) {
        return pathToClassList
                .stream()
                .map(s -> {
                    try {
                        return Class.forName(s);
                    } catch (ClassNotFoundException e) {
                        log.error(e.getMessage());
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());
    }
}
