package com.kafka.example.retry.utils;

import com.kafka.example.retry.models.Consumer;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@Component
@ConfigurationProperties("kafka-retry")
public class KafkaPropertiesProvider {
    private String topicRetryPrefix;
    private int maxRetries;
    private List<Consumer> consumers;
    private ExceptionList exceptions;

    @Data
    @ConfigurationProperties("exceptions")
    public static class ExceptionList {
        private List<String> recoverable = new ArrayList<>();
        private List<String> unrecoverable = new ArrayList<>();
    }
}
