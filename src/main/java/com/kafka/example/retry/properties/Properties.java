package com.kafka.example.retry.properties;

import com.kafka.example.retry.entities.Consumer;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Getter
@Setter
@Builder(setterPrefix = "with")
public class Properties {
    private int maxRetries;
    @Singular
    private List<Consumer> consumers;
}

