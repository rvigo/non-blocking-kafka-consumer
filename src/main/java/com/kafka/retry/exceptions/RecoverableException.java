package com.kafka.retry.exceptions;


public class RecoverableException extends RuntimeException {

    private String message;

    public RecoverableException(String message, Exception ex) {
        super(message);
        this.message = message;
    }

    public RecoverableException(String message) {
        super(message);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}