package retry.managers;

import com.kafka.example.retry.exceptions.PartitionManagementException;
import com.kafka.example.retry.managers.KafkaConsumerManager;
import com.kafka.example.retry.entities.Consumer;
import com.kafka.example.retry.entities.KafkaTopic;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;


@ExtendWith(SpringExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class KafkaConsumerManagerTest {

    @MockBean
    private KafkaListenerEndpointRegistry registry;
    @MockBean
    private MessageListenerContainer messageListenerContainer;
    private KafkaConsumerManager consumerManager;

    private static final String REGISTRY_ID = "TEST-ID";
    private static final Long CONSUMER_DELAY = 1L;
    private static final String TOPIC_NAME = "test-topic";
    private static final int TOPIC_PARTITION = 0;
    private static final int RETRY_VALUE = 0;

    private final KafkaTopic topic = new KafkaTopic(TOPIC_NAME, 1);

    @BeforeEach
    public void setUp() {
        topic.setTopicName(TOPIC_NAME);
        topic.setRetryValue(RETRY_VALUE);

        Consumer consumer = new Consumer();
        consumer.setId(REGISTRY_ID);
        consumer.setDelay(CONSUMER_DELAY);
        consumer.setTopics(of(topic));

        consumerManager = new KafkaConsumerManager(registry, of(consumer));
    }

    @Test
    public void shouldSleepConsumerPartition() {
        Mockito.when(registry.getListenerContainer(REGISTRY_ID)).thenReturn(messageListenerContainer);
        Mockito.doNothing().when(messageListenerContainer).pausePartition(Mockito.any(TopicPartition.class));
        Mockito.when(messageListenerContainer.isPartitionPauseRequested(Mockito.any(TopicPartition.class))).thenReturn(true);
        Mockito.doNothing().when(messageListenerContainer).resumePartition(Mockito.any(TopicPartition.class));

        long firstAttemptTimeStamp = System.currentTimeMillis() + 5L;

        consumerManager.sleep(TOPIC_NAME, TOPIC_PARTITION, firstAttemptTimeStamp);
        Mockito.verify(messageListenerContainer, times(1)).pausePartition(Mockito.any(TopicPartition.class));
    }

    @Test
    public void shouldReturnConsumerDelayByTopicName() {
        long firstAttemptTimeStamp = System.currentTimeMillis() - 100000L;
        boolean response = consumerManager.shouldConsume(TOPIC_NAME, firstAttemptTimeStamp);
        assertTrue(response);
        firstAttemptTimeStamp = System.currentTimeMillis() + 100000L;
        response = consumerManager.shouldConsume(TOPIC_NAME, firstAttemptTimeStamp);
        assertFalse(response);
    }

    @Test
    public void shouldCatchAnErrorWhilePausingPartition() {
        Mockito.doThrow(PartitionManagementException.class).when(messageListenerContainer).pausePartition(Mockito.any(TopicPartition.class));
        Exception exception = assertThrows(PartitionManagementException.class, () -> consumerManager.sleep(TOPIC_NAME, TOPIC_PARTITION, System.currentTimeMillis()));
        assertNotNull(exception);
        assertEquals("An error occurred while pausing the desired partition", exception.getMessage());
    }

    @Test
    public void shouldCatchAnErrorWhileResumingPartition() {
        Mockito.when(registry.getListenerContainer(REGISTRY_ID)).thenReturn(messageListenerContainer);
        Mockito.when(messageListenerContainer.isPartitionPauseRequested(Mockito.any(TopicPartition.class))).thenReturn(true);
        Mockito.doNothing().when(messageListenerContainer).pausePartition(Mockito.any(TopicPartition.class));
        Mockito.doThrow(PartitionManagementException.class).when(messageListenerContainer).resumePartition(Mockito.any(TopicPartition.class));
        long firstAttemptTimeStamp = System.currentTimeMillis() + 5L;

        Exception exception = assertThrows(PartitionManagementException.class, () -> consumerManager.sleep(TOPIC_NAME, TOPIC_PARTITION, firstAttemptTimeStamp));
        assertNotNull(exception);
        assertEquals("An error occurred while resuming the desired partition", exception.getMessage());
    }
}
