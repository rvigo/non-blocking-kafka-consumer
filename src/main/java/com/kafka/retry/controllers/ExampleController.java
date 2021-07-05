package com.kafka.retry.controllers;

import com.kafka.retry.configurations.KafkaTopicHolder;
import com.kafka.retry.producers.ExampleProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class ExampleController {
    private ExampleProducer producer;
    private KafkaTopicHolder kafkaTopicHolder;

    @Autowired
    public ExampleController(ExampleProducer producer, KafkaTopicHolder kafkaTopicHolder) {
        this.producer = producer;
        this.kafkaTopicHolder = kafkaTopicHolder;
    }

    @PostMapping("/post_message/a/{valueOfA}/b/{valueOfB}/c/{valueOfC}/d/{valueOfD}")
    public void postMessage(@PathVariable Integer valueOfA,
                            @PathVariable Integer valueOfB,
                            @PathVariable Integer valueOfC,
                            @PathVariable Integer valueOfD) {

        int a = Optional.ofNullable(valueOfA).orElse(0);
        int b = Optional.ofNullable(valueOfB).orElse(0);
        int c = Optional.ofNullable(valueOfC).orElse(0);
        int d = Optional.ofNullable(valueOfD).orElse(0);

        producer.send(a, b, c, d);
    }
    @GetMapping("/get")
    public ResponseEntity<String> getTopicsName(){
        return ResponseEntity.ok(kafkaTopicHolder.toString());
    }
}
