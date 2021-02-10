package com.kafka.retry.configurations;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Component
public class KafkaManager {

    private final KafkaListenerEndpointRegistry registry;

    @Autowired
    public KafkaManager(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void pause(String id) {
        registry.getListenerContainer(id).pause();
    }

    public void resume(String id) {
        registry.getListenerContainer(id).resume();
    }
}

