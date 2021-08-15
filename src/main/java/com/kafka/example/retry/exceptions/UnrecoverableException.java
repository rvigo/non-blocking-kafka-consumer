package com.kafka.example.retry.exceptions;

import lombok.Getter;

@Getter
public class UnrecoverableException extends RuntimeException {
    private final String message;
    private final Throwable cause;

    public UnrecoverableException() {
        super();
        this.message = null;
        this.cause = null;
    }

    public UnrecoverableException(String message) {
        super(message);
        this.message = message;
        this.cause = null;
    }

    public UnrecoverableException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
        this.cause = cause;
    }
}