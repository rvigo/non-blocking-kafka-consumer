package com.kafka.retry.models;

import java.util.Objects;

public class KafkaTopic {
    private String topicName;
    private Integer retryValue;
    private KafkaTopic nextTopic;
    private KafkaTopic previousTopic;
    private Type type;

    public KafkaTopic(String topicName, Integer retryValue, Type type) {
        this.topicName = topicName;
        this.retryValue = retryValue;
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Integer getRetryValue() {
        return retryValue;
    }

    public void setRetryValue(Integer retryValue) {
        this.retryValue = retryValue;
    }

    public KafkaTopic getNextTopic() {
        return nextTopic;
    }

    public void setNextTopic(KafkaTopic nextTopic) {
        this.nextTopic = nextTopic;
    }

    public KafkaTopic getPreviousTopic() {
        return previousTopic;
    }

    public void setPreviousTopic(KafkaTopic previousTopic) {
        this.previousTopic = previousTopic;
    }

    public String toString() {
        return String.format("Topic name: %s, Retry count: %s", topicName, retryValue);
    }

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

    public enum Type {
        MAIN, RETRY, DLT
    }
}
