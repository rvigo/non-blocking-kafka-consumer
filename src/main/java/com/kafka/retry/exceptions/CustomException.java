package com.kafka.retry.exceptions;

public class CustomException extends RuntimeException {

    private final String message;

    public CustomException(String message, Exception ex) {
        super(message);
        this.message = message;
    }

    public CustomException(String message) {
        super(message);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}