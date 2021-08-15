package com.kafka.example.retry.properties;

import com.kafka.example.retry.entities.Consumer;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Getter
@Setter
@ConfigurationProperties("kafka-retry")
public class KafkaPropertiesProvider {
    private String topicRetryPrefix;
    private int maxRetries = 0;
    private List<Consumer> consumers;
}
