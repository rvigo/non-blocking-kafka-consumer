package com.kafka.example.retry.utils;

import com.kafka.example.retry.models.KafkaTopic;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@Component
@ConfigurationProperties("kafka-retry")
public class KafkaPropertiesProvider {
    private String topicRetryPrefix;
    private List<KafkaTopic> topics = new ArrayList<>();
    private List<String> consumerIds = new ArrayList<>();
    private List<Long> delays = new ArrayList<>();
    private int maxRetries;
    private ExceptionList exceptions;
    private List<Consumer> consumers;

    @Data
    @ConfigurationProperties("exceptions")
    public static class ExceptionList {
        private List<String> recoverable = new ArrayList<>();
        private List<String> unrecoverable = new ArrayList<>();
    }

    @Data
    @NoArgsConstructor
    public static class Consumer{
        private String id;
        private long delay;
        private List<KafkaTopic> topics;
    }
}
