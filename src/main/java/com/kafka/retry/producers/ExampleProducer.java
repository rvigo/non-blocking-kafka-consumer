package com.kafka.retry.producers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.ExecutionException;

@Service
public class ExampleProducer {
    @Value("${topics.my_topic}")
    private String topic;

    private KafkaTemplate kafkaTemplate;
    private static final Integer ID = 8;

    @Autowired
    public ExampleProducer(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String message) {
        try {
            generateNewMessages(message, 10);
        } catch (Exception e) {
            throw new RuntimeException("An error was thrown while sending the message - " + e.getMessage());
        }
    }

    private void generateNewMessages(String message, int range) {
        Random rand = new Random();

        for (int i = 0; i < range; i++) {
            int randomId = rand.nextInt(10);
            kafkaTemplate.send(topic, String.valueOf(randomId), message + i);
        }
    }
}
