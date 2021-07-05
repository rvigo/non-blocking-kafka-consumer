package com.kafka.retry.configurations;

import com.kafka.retry.models.KafkaTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.kafka.retry.models.KafkaTopic.Type.*;

@Configuration
@ConfigurationProperties(prefix = "retry")
public class KafkaTopicHolderConfiguration {
    private String mainTopic;
    private String dlqTopic;
    private String firstRetryTopic;
    private String secondRetryTopic;
    private String thirdRetryTopic;

    //TODO refatorar pra criar classe em tempo de execução
    @Bean
    public KafkaTopicHolder kafkaTopicManagerFactory(KafkaTopicHolder manager) {
        KafkaTopic main = new KafkaTopic(mainTopic, 0, MAIN);
        KafkaTopic first = new KafkaTopic(firstRetryTopic, 1, RETRY);
        KafkaTopic second = new KafkaTopic(secondRetryTopic, 2, RETRY);
        KafkaTopic third = new KafkaTopic(thirdRetryTopic, 3, RETRY);
        KafkaTopic dlq = new KafkaTopic(dlqTopic, 4, DLT);

        manager.add(main);
        manager.add(first);
        manager.add(second);
        manager.add(third);
        manager.add(dlq);

        return manager;
    }
}
