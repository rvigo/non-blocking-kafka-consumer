package com.kafka.example.retry.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaExceptionRecover {
    private final List<Class<?>> recoverableExceptionList;

    @Autowired
    public KafkaExceptionRecover(Properties properties) {
        this.recoverableExceptionList = properties.getRecoverableExceptions();
    }

    public boolean isRecoverableException(Exception exception) {
        Class<? extends Throwable> clazz = exception.getCause().getClass();
        return this.recoverableExceptionList.contains(clazz);
    }
}
