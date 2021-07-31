package com.kafka.example.retry.models;

import lombok.*;

import java.util.Objects;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@ToString(onlyExplicitlyIncluded = true)
public class KafkaTopic {
    @NonNull
    @ToString.Include
    private String topicName;
    @NonNull
    @ToString.Include
    private Integer retryValue;
    private KafkaTopic nextTopic;
    private KafkaTopic previousTopic;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaTopic that = (KafkaTopic) o;
        return topicName.equals(that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName);
    }
}

