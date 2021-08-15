package com.kafka.example.retry.properties;

import com.kafka.example.retry.entities.Consumer;
import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder(setterPrefix = "with")
public class Properties {
    private int maxRetries;
    @Singular
    private List<Consumer> consumers;
}

