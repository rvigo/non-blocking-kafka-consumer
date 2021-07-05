package com.kafka.retry.exceptions;

public class UnregisteredTopicException extends RuntimeException {

    private String message;

    public UnregisteredTopicException() {
        super();
    }

    public UnregisteredTopicException(String message) {
        super(message);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}