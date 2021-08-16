package com.kafka.example.retry.entities;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class Consumer {
    private String id;
    private long delay;
    private List<KafkaTopic> topics;
}

