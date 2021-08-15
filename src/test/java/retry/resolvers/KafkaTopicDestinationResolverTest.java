package retry.resolvers;

import com.kafka.example.retry.exceptions.RecoverableException;
import com.kafka.example.retry.exceptions.UnrecoverableException;
import com.kafka.example.retry.managers.KafkaTopicChain;
import com.kafka.example.retry.entities.KafkaTopic;
import com.kafka.example.retry.properties.Properties;
import com.kafka.example.retry.resolvers.KafkaTopicDestinationResolver;
import com.kafka.example.retry.utils.KafkaExceptionRecover;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class KafkaTopicDestinationResolverTest {

    private static final String MAIN_TOPIC = "test-topic";
    private static final String FIRST_RETRY_TOPIC = "first-retry-test-topic";
    private static final String SECOND_RETRY_TOPIC = "second-retry-test-topic";
    private static final String THIRD_RETRY_TOPIC = "third-retry-test-topic";
    private static final String DLQ_TOPIC = "dlq-test-topic";

    private ConsumerRecord<String, String> consumerRecord;
    private static final long OFFSET = 1000L;
    private static final int PARTITION = 1;
    private static final String KEY = "key";
    private static final String VALUE = "value";

    private KafkaTopicDestinationResolver resolver;
    private final RecoverableException recoverableException = new RecoverableException();
    private final UnrecoverableException unrecoverableException = new UnrecoverableException();

    @MockBean
    private Properties properties;

    @BeforeEach
    public void setUp() {
        KafkaTopic mainTopic = new KafkaTopic(MAIN_TOPIC, 0);
        KafkaTopic firstRetryTopic = new KafkaTopic(FIRST_RETRY_TOPIC, 1);
        KafkaTopic secondRetryTopic = new KafkaTopic(SECOND_RETRY_TOPIC, 2);
        KafkaTopic thirdRetryTopic = new KafkaTopic(THIRD_RETRY_TOPIC, 3);
        KafkaTopic dlqTopic = new KafkaTopic(DLQ_TOPIC, 4);
        KafkaTopicChain kafkaTopicChain = new KafkaTopicChain();
        Stream.of(mainTopic, firstRetryTopic, secondRetryTopic, thirdRetryTopic, dlqTopic).forEach(kafkaTopicChain::add);

        KafkaExceptionRecover exceptionRecover = new KafkaExceptionRecover();
        consumerRecord = new ConsumerRecord<>(MAIN_TOPIC, PARTITION, OFFSET, KEY, VALUE);
        resolver = new KafkaTopicDestinationResolver(properties, kafkaTopicChain, exceptionRecover);

        Mockito.when(properties.getMaxRetries()).thenReturn(4);
    }

    @Test
    public void shouldResolveNextTopicFromMainTopic() {
        TopicPartition topicPartition = resolver.resolveMainTopicDestination().apply(consumerRecord, recoverableException);
        assertEquals(FIRST_RETRY_TOPIC, topicPartition.topic());
        assertEquals(PARTITION, topicPartition.partition());
    }

    @Test
    public void shouldResolveDlqFromMainTopicWhenCaughtAUnrecoverableException() {
        TopicPartition topicPartition = resolver.resolveMainTopicDestination().apply(consumerRecord, unrecoverableException);
        assertEquals(DLQ_TOPIC, topicPartition.topic());
        assertEquals(PARTITION, topicPartition.partition());
    }

    @Test
    public void shouldResolveDlqFromMainTopicWhenMaxRetriesEqualsZero() {
        Mockito.when(properties.getMaxRetries()).thenReturn(0);
        TopicPartition topicPartition = resolver.resolveMainTopicDestination().apply(consumerRecord, recoverableException);
        assertEquals(DLQ_TOPIC, topicPartition.topic());
        assertEquals(PARTITION, topicPartition.partition());
    }

    @Test
    public void shouldResolveNextTopicFromRetry() {
        consumerRecord = new ConsumerRecord<>(FIRST_RETRY_TOPIC, PARTITION, OFFSET, KEY, VALUE);
        TopicPartition topicPartition = resolver.resolveRetryTopicDestination().apply(consumerRecord, recoverableException);
        assertEquals(SECOND_RETRY_TOPIC, topicPartition.topic());
        assertEquals(PARTITION, topicPartition.partition());
    }

    @Test
    public void shouldResolveDlqFromRetryTopicWhenReachMaxRetriesAttempt() {
        Mockito.when(properties.getMaxRetries()).thenReturn(1);
        consumerRecord = new ConsumerRecord<>(THIRD_RETRY_TOPIC, PARTITION, OFFSET, KEY, VALUE);
        TopicPartition topicPartition = resolver.resolveRetryTopicDestination().apply(consumerRecord, recoverableException);
        assertEquals(DLQ_TOPIC, topicPartition.topic());
        assertEquals(PARTITION, topicPartition.partition());
    }
}

