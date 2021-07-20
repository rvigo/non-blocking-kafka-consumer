package com.kafka.retry.configurations.kafka.utils;

import com.kafka.retry.models.KafkaTopic;
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
    private List<KafkaTopic> topics;
    private List<String> consumerIds;
    private List<Long> delays;
    private int maxRetries;
    @Singular
    private List<Class<?>> recoverableExceptions;
    @Singular
    private List<Class<?>> unrecoverableExceptions;
}

