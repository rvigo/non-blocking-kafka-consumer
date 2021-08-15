package com.kafka.example.retry.resolvers;

import com.kafka.example.retry.managers.KafkaTopicChain;
import com.kafka.example.retry.utils.KafkaExceptionRecover;
import com.kafka.example.retry.properties.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaTopicDestinationResolver {
    private final Properties kafkaProperties;
    private final KafkaTopicChain kafkaTopicChain;
    private final KafkaExceptionRecover kafkaExceptionRecover;

    // resolve topic destination for the main consumer
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> resolveMainTopicDestination() {
        return (record, exception) -> {
            if (kafkaExceptionRecover.isUnrecoverableException(exception) || kafkaProperties.getMaxRetries() == 0) {
                log.debug(String.format("sending message with id %s to dlt", record.key()));
                return new TopicPartition(kafkaTopicChain.getLastTopic().getTopicName(), record.partition());
            }

            String source = record.topic() != null && !record.topic().isEmpty() ? record.topic() : kafkaTopicChain.getFirstTopic().getTopicName();
            String target = kafkaTopicChain.getKafkaTopicByName(source).getNextTopic().getTopicName();
            return new TopicPartition(target, record.partition());
        };
    }

    //resolve topic destination for retries consumers
    public BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> resolveRetryTopicDestination() {
        return (record, exception) -> {
            String target;
            int actualRetryCount = kafkaTopicChain.getKafkaTopicByName(record.topic()).getRetryValue();
            if (actualRetryCount < kafkaProperties.getMaxRetries()) {
                target = kafkaTopicChain.getKafkaTopicByRetryValue(actualRetryCount).getNextTopic().getTopicName();
            } else {
                log.debug(String.format("sending message %s to dlt", record.key()));
                target = kafkaTopicChain.getLastTopic().getTopicName();
            }
            return new TopicPartition(target, record.partition());
        };
    }
}
