package com.kafka.example.controllers;

import com.kafka.example.producers.ExampleProducer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@Slf4j
@AllArgsConstructor
@RestController
public class ExampleController {
    private ExampleProducer producer;

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
        log.info("all messages sent");
    }
}
