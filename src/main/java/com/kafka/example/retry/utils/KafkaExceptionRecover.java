package com.kafka.example.retry.utils;

import com.kafka.example.retry.exceptions.UnrecoverableException;
import org.springframework.stereotype.Component;

@Component
public class KafkaExceptionRecover {
    public boolean isUnrecoverableException(Exception exception) {
       return exception instanceof UnrecoverableException;
    }
}
