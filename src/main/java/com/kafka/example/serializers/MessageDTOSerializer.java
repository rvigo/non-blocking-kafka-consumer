package com.kafka.example.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.example.dtos.MessageDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

@Slf4j
public class MessageDTOSerializer implements Serializer<MessageDTO> {
    @Override
    public byte[] serialize(String topic, MessageDTO data) {
        byte[] res = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            res = objectMapper.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }
        return res;
    }
}
