package com.kafka.retry.services;

import com.kafka.retry.exceptions.NonRecoverableException;
import com.kafka.retry.exceptions.RecoverableException;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class ExampleService {
    public void process(String message, String id) {
        Integer parsedId = Integer.parseInt(id);

        if (parsedId.equals(0))
            System.out.printf(LocalDateTime.now() + " - " + id + " - the message is " + message);
        else if (parsedId > 0 && parsedId <= 5)
            throw new RecoverableException(LocalDateTime.now() + " - " + id + " - this is a recoverable exception, starting recovery flow");
        else if (parsedId > 6)
            throw new NonRecoverableException(LocalDateTime.now() + " - " + id + " - this is a non recoverable exception, the message will be sent to DLT");

    }
}
