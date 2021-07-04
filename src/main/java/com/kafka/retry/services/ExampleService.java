package com.kafka.retry.services;

import com.kafka.retry.exceptions.NonRecoverableException;
import com.kafka.retry.exceptions.RecoverableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class ExampleService {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    public void process(String message, String id) {
        Integer parsedId = Integer.parseInt(id);

        if (parsedId.equals(0)) {
            logger.info(id + " - this is a recoverable exception, starting recovery flow");
            throw new RecoverableException(LocalDateTime.now() + " - " + id + " - this is a recoverable exception, starting recovery flow");
        } else if (parsedId > 0 && parsedId <= 5) {
            logger.info(id + " - the message is " + message);
        } else {
            logger.info(id + " - this is a non recoverable exception, the message will be sent to DLT");
            throw new NonRecoverableException(LocalDateTime.now() + " - " + id + " - this is a non recoverable exception, the message will be sent to DLT");
        }
    }
}
