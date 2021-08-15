package com.kafka.example.retry.exceptions;

import lombok.Getter;

@Getter
public class PartitionManagementException extends RuntimeException {
    private final String message;
    private final Throwable cause;

    public PartitionManagementException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
        this.cause = cause;
    }
}
