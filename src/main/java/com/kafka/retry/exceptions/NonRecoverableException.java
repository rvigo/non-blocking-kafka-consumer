package com.kafka.retry.exceptions;


public class NonRecoverableException extends RuntimeException {

    private String message;

    public NonRecoverableException(String message, Exception ex) {
        super(message);
        this.message = message;
    }

    public NonRecoverableException(String message) {
        super(message);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}