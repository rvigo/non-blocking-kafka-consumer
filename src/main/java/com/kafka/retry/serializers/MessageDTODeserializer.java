package com.kafka.retry.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.retry.dtos.MessageDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class MessageDTODeserializer implements Deserializer<MessageDTO> {
    @Override
    public MessageDTO deserialize(String topic, byte[] data) {
        MessageDTO messageDTO = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            messageDTO = objectMapper.readValue(data, MessageDTO.class);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return messageDTO;
    }
}
