package com.kafka.example.retry.models;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class Consumer {
    private String id;
    private long delay;
    private List<KafkaTopic> topics;
}

