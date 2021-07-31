package com.kafka.example.retry.utils;

import com.kafka.example.retry.models.Consumer;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Data
@Slf4j
@Builder(setterPrefix = "with")
public class Properties {
    private String topicPrefix;
    private int maxRetries;
    @Singular
    private List<Class<?>> recoverableExceptions;
    @Singular
    private List<Class<?>> unrecoverableExceptions;
    @Singular
    private List<Consumer> consumers;
}

