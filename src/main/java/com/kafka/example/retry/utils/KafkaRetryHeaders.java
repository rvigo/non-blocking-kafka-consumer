package com.kafka.example.retry.utils;

import org.springframework.stereotype.Component;

@Component
public class KafkaRetryHeaders {
    public static final String LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER = "LAST_RETRY_ATTEMPT_TIMESTAMP";
    public static final String FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER = "FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER";
}
