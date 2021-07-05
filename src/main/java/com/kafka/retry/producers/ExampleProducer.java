package com.kafka.retry.producers;

import com.kafka.retry.dtos.MessageDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.stream.Stream.of;

@Service
public class ExampleProducer {
    private final KafkaTemplate<String, MessageDTO> kafkaTemplate;
    @Value("${topics.main.topic}")
    private String topic;

    @Autowired
    public ExampleProducer(KafkaTemplate<String, MessageDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(int a, int b, int c, int d) {
        try {
            List<MessageDTO> messageDTOList = of(getA(a), getB(b), getC(c), getD(d))
                    .flatMap(List::stream)
                    .collect(Collectors.toList());

            sendKafkaMessages(messageDTOList);

        } catch (Exception e) {
            throw new RuntimeException("An error was thrown while sending the message", e);
        }
    }

    private void sendKafkaMessages(List<MessageDTO> messageDTOList) {
        for (MessageDTO dto : messageDTOList) {
            kafkaTemplate.send(topic, dto.getId().toString(), dto);
        }
    }

    private List<MessageDTO> getA(int iterations) {
        List<MessageDTO> messageDTOList = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            messageDTOList.add(new MessageDTO(i, "A", UUID.randomUUID()));
        }
        return messageDTOList;
    }

    private List<MessageDTO> getB(int iterations) {
        List<MessageDTO> messageDTOList = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            messageDTOList.add(new MessageDTO(i, "B", UUID.randomUUID()));
        }
        return messageDTOList;
    }

    private List<MessageDTO> getC(int iterations) {
        List<MessageDTO> messageDTOList = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            messageDTOList.add(new MessageDTO(i, "C", UUID.randomUUID()));
        }
        return messageDTOList;
    }

    private List<MessageDTO> getD(int iterations) {
        List<MessageDTO> messageDTOList = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            messageDTOList.add(new MessageDTO(i, "D", UUID.randomUUID()));
        }
        return messageDTOList;
    }
}
