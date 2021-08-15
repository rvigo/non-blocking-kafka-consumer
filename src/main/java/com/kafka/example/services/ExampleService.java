package com.kafka.example.services;

import com.kafka.example.dtos.MessageDTO;
import com.kafka.example.retry.exceptions.UnrecoverableException;
import com.kafka.example.retry.exceptions.RecoverableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
public class ExampleService {
    public void process(MessageDTO message) {
        switch (message.getOriginName()) {
            case "A":
                log.info(String.format("The message with id %s was successfully processed", message.getId()));
                break;
            case "B":
                getRandomDestiny(message.getId());
                log.info(String.format("The message with id %s was successfully processed", message.getId()));
                break;
            case "C":
                log.error(String.format("a recoverable error was caught while processing message %s, starting recovery flow", message.getId()));
                throw new RecoverableException();
            default:
                log.error(String.format("a non recoverable exception was caught while processing message %s, sending message to dlq", message.getId()));
                throw new UnrecoverableException();
        }
    }

    private void getRandomDestiny(UUID messageId) {
        List<Integer> givenList = Arrays.asList(1, 2);
        Random rand = new Random();
        int randomElement = givenList.get(rand.nextInt(givenList.size()));
        if (randomElement == 1) {
            log.error(String.format("a recoverable error was caught while processing message %s, starting recovery flow", messageId));
            throw new RecoverableException();
        }
    }
}
