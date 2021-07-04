package com.kafka.retry.models;

public class KafkaTopicHolder {
    private String topicName;
    private Integer retryCount;
    private KafkaTopicHolder nextTopic;
    private KafkaTopicHolder previousTopic;

    public KafkaTopicHolder(String topicName, Integer retryCount) {
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

    public KafkaTopicHolder getNextTopic() {
        return nextTopic;
    }

    public void setNextTopic(KafkaTopicHolder nextTopic) {
        this.nextTopic = nextTopic;
    }

    public KafkaTopicHolder getPreviousTopic() {
        return previousTopic;
    }

    public void setPreviousTopic(KafkaTopicHolder previousTopic) {
        this.previousTopic = previousTopic;
    }

    public String toString() {
        return String.format("Topic name: %s, Retry count: %s", topicName, retryCount);
    }
}
