package com.kafka.example.exceptions;

import lombok.Getter;

@Getter
public class CustomException extends RuntimeException {

    private final String message;
    private final Throwable cause;

    public CustomException(String message) {
        super(message);
        this.message = message;
        this.cause = null;
    }

    public CustomException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
        this.cause = cause;
    }
}