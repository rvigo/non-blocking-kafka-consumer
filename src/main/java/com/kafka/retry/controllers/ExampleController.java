package com.kafka.retry.controllers;

import com.kafka.retry.producers.ExampleProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ExampleController {

    @Autowired
    private ExampleProducer producer;

    @PostMapping("/post_message")
    public void postMessage(){
        producer.send("example message");
    }
}
