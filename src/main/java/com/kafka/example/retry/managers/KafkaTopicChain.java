package com.kafka.example.retry.managers;

import com.kafka.example.retry.exceptions.UnregisteredTopicException;
import com.kafka.example.retry.entities.KafkaTopic;

import static java.lang.String.format;

public class KafkaTopicChain {
    private KafkaTopic firstTopic;
    private KafkaTopic lastTopic;
    private int size;

    public KafkaTopicChain() {
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

    public void remove(KafkaTopic kafkaTopic) {
        KafkaTopic currentTopic = firstTopic;
        while (currentTopic != null
                && currentTopic.getNextTopic() != null
                || lastTopic.equals(currentTopic)) {
            if (currentTopic.equals(kafkaTopic)) {
                if (size == 1) {
                    firstTopic = null;
                    lastTopic = null;
                } else if (currentTopic.equals(firstTopic)) {
                    firstTopic.getNextTopic().setPreviousTopic(null);
                    firstTopic = firstTopic.getNextTopic();
                } else if (currentTopic.equals(lastTopic)) {
                    lastTopic = lastTopic.getPreviousTopic();
                    lastTopic.setNextTopic(null);
                } else {
                    currentTopic.getPreviousTopic().setNextTopic(currentTopic.getNextTopic());
                    currentTopic.getNextTopic().setPreviousTopic(currentTopic.getPreviousTopic());
                }
                size--;
                break;
            }
            currentTopic = currentTopic.getNextTopic();
        }
    }

    public boolean contains(KafkaTopic topic){
        KafkaTopic current = firstTopic;
        while (null != current) {
            if (current.equals(topic)) {
                return true;
            }
            current = current.getNextTopic();
        }
        return false;
    }

    public KafkaTopic getKafkaTopicByName(String topic) {
        KafkaTopic current = firstTopic;
        while (null != current) {
            if (current.getTopicName().equals(topic)) {
                return current;
            }
            current = current.getNextTopic();
        }
        throw new UnregisteredTopicException();
    }

    public KafkaTopic getKafkaTopicByRetryValue(Integer retryCount) {
        KafkaTopic currentTopic = firstTopic;
        while (null != currentTopic) {
            if (currentTopic.getRetryValue().equals(retryCount)) {
                return currentTopic;
            }
            currentTopic = currentTopic.getNextTopic();
        }
        throw new UnregisteredTopicException();
    }

    public int size() {
        return size;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        KafkaTopic topic = firstTopic;
        int i = 0;
        while (topic != null) {
            sb.append(format("%s : %s\n", i, topic));
            topic = topic.getNextTopic();
            i++;
        }
        return sb.toString();
    }
}
