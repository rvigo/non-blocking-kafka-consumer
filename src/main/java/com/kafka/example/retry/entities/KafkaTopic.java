package com.kafka.example.retry.entities;

import lombok.*;

@Data
@RequiredArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class KafkaTopic {
    @NonNull
    @ToString.Include
    @EqualsAndHashCode.Include
    private String topicName;
    @NonNull
    @ToString.Include
    @EqualsAndHashCode.Include
    private Integer retryValue;
    private KafkaTopic nextTopic;
    private KafkaTopic previousTopic;
}

