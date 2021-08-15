package com.kafka.example.producers;

import com.kafka.example.retry.managers.KafkaTopicChain;
import com.kafka.example.dtos.MessageDTO;
import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.stream.Stream.of;

@Service
@AllArgsConstructor
public class ExampleProducer {
    private final KafkaTemplate<String, MessageDTO> kafkaTemplate;
    private final KafkaTopicChain holder;

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
            kafkaTemplate.send(holder.getFirstTopic().getTopicName(), dto.getId().toString(), dto);
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
