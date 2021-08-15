package com.kafka.example.retry.exceptions;

import lombok.Getter;

@Getter
public class RecoverableException extends RuntimeException {
    private final String message;
    private final Throwable cause;

    public RecoverableException() {
        super();
        this.message = null;
        this.cause = null;
    }

    public RecoverableException(String message) {
        super(message);
        this.message = message;
        this.cause = null;
    }

    public RecoverableException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
        this.cause = cause;
    }
}