package com.kafka.retry.models;

public class KafkaTopic {
    private String topicName;
    private Integer retryCount;
    private KafkaTopic nextTopic;
    private KafkaTopic previousTopic;

    public KafkaTopic(String topicName, Integer retryCount) {
        this.topicName = topicName;
        this.retryCount = retryCount;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
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
        return String.format("Topic name: %s, Retry count: %s", topicName, retryCount);
    }
}
