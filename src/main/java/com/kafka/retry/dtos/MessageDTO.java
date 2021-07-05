package com.kafka.retry.dtos;

import java.util.UUID;

public class MessageDTO {
    private int value;
    private String originName;
    private UUID id;

    public MessageDTO() {
    }

    public MessageDTO(int value, String originName, UUID id) {
        this.value = value;
        this.originName = originName;
        this.id = id;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getOriginName() {
        return originName;
    }

    public void setOriginName(String originName) {
        this.originName = originName;
    }
}
