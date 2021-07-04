package com.kafka.retry.configurations.managers;

import com.kafka.retry.models.KafkaTopic;
import org.springframework.stereotype.Component;

@Component
public class KafkaTopicManager {
    private KafkaTopic firstTopic;
    private KafkaTopic lastTopic;
    private int size;

    public KafkaTopicManager() {
        firstTopic = null;
        lastTopic = null;
    }

    public KafkaTopic getFirstTopic() {
        return firstTopic;
    }

    public KafkaTopic getLastTopic() {
        return lastTopic;
    }

    public void add(KafkaTopic kafkaTopic) {
        if (size != 0) {
            lastTopic.setNextTopic(kafkaTopic);
            kafkaTopic.setPreviousTopic(lastTopic);

            lastTopic = kafkaTopic;
        } else {
            lastTopic = kafkaTopic;
            firstTopic = lastTopic;
        }
        size++;
    }

    public boolean contains(String topic) {
        KafkaTopic current = firstTopic;
        while (null != current) {
            if (current.getTopicName().equals(topic)) {
                return true;
            }
            current = current.getNextTopic();
        }
        return false;
    }

    public KafkaTopic getTopicByName(String topic) {
        KafkaTopic current = firstTopic;
        while (null != current) {
            if (current.getTopicName().equals(topic)) {
                return current;
            }
            current = current.getNextTopic();
        }
        return lastTopic;
    }

    public KafkaTopic getTopicByRetryCount(Integer retryCount) {
        KafkaTopic currentTopic = firstTopic;
        while (null != currentTopic) {
            if (currentTopic.getRetryCount().equals(retryCount)) {
                return currentTopic;
            }
            currentTopic = currentTopic.getNextTopic();
        }
        return lastTopic;
    }

    public int size() {
        return size;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        KafkaTopic topic = firstTopic;
        int i = 0;
        while (topic != null) {
            sb.append(String.format("%s : %s\n", i, topic));
            topic = topic.getNextTopic();
            i++;
        }
        return sb.toString();
    }
}
