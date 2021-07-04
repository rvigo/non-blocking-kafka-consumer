package com.kafka.retry.producers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ExampleProducer {
    @Value("${topics.main.topic}")
    private String topic;

    private KafkaTemplate kafkaTemplate;

    @Autowired
    public ExampleProducer(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String message) {
        try {
            generateNewMessages(message, 1);
        } catch (Exception e) {
            throw new RuntimeException("An error was thrown while sending the message - " + e.getMessage());
        }
    }

    private void generateNewMessages(String message, int range) {
        for (int i = 0; i < range; i++) {
            kafkaTemplate.send(topic, UUID.randomUUID().toString(), message + " " + i);
        }
    }
}
