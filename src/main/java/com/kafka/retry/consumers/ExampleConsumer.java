package com.kafka.retry.consumers;

import com.kafka.retry.configurations.KafkaManager;
import com.kafka.retry.services.ExampleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class ExampleConsumer {

    private KafkaManager kafkaManager;
    private ExampleService service;

    private static final String TOPIC = "my_topic";
    private static final String DELAYED_LISTENER_1_ID = "RETRY_1";
    private static final String DELAYED_LISTENER_2_ID = "RETRY_2";
    private static final String RETRY_1 = TOPIC + "_group_id_RETRY_1";
    private static final String RETRY_2 = TOPIC + "_group_id_RETRY_2";

    @Autowired
    public ExampleConsumer(KafkaManager kafkaManager, ExampleService service) {
        this.kafkaManager = kafkaManager;
        this.service = service;
    }

    @KafkaListener(topics = TOPIC, groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        service.process(message, key);
    }

    @KafkaListener(id = DELAYED_LISTENER_1_ID, topics = RETRY_1,
            groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaRetryListenerContainerFactory")
    public void firstDelayedRetry(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) throws Exception {

        System.out.println(LocalDateTime.now() + " - " + key + " - pausing the consumer for 5s");
        kafkaManager.pause(DELAYED_LISTENER_1_ID);
        Thread.sleep(5000l);
        System.out.println(LocalDateTime.now() + " - " + key + " - resuming the consumer for 5s");
        kafkaManager.resume(DELAYED_LISTENER_1_ID);

        service.process(message, key);

    }

    @KafkaListener(id = DELAYED_LISTENER_2_ID, topics = RETRY_2,
            groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaRetryListenerContainerFactory")
    public void secondDelayedRetry(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) throws Exception {

        System.out.println(LocalDateTime.now() + " - " + key + " - pausing the consumer for 15s");
        kafkaManager.pause(DELAYED_LISTENER_1_ID);
        Thread.sleep(15000l);
        System.out.println(LocalDateTime.now() + " - " + key + " - resuming the consumer for 15s");
        kafkaManager.resume(DELAYED_LISTENER_1_ID);

        service.process(message, key);

    }
}

