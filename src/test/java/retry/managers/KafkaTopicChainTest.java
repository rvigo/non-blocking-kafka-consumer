package retry.managers;

import com.kafka.example.retry.exceptions.UnregisteredTopicException;
import com.kafka.example.retry.managers.KafkaTopicChain;
import com.kafka.example.retry.entities.KafkaTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.junit.jupiter.SpringExtension;


import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class KafkaTopicChainTest {

    private KafkaTopicChain kafkaTopicChain;

    private static final String MAIN_TOPIC = "test-topic";
    private static final String FIRST_RETRY_TOPIC = "first-retry-test-topic";
    private static final String SECOND_RETRY_TOPIC = "second-retry-test-topic";
    private static final String THIRD_RETRY_TOPIC = "third-retry-test-topic";
    private static final String DLQ_TOPIC = "dlq-test-topic";
    private static final String INVALID_TOPIC_NAME = "invalid";
    private KafkaTopic mainTopic;
    private KafkaTopic firstRetryTopic;
    private KafkaTopic secondRetryTopic;
    private KafkaTopic thirdRetryTopic;
    private KafkaTopic dlqTopic;

    @BeforeEach
    public void setUp() {
        mainTopic = new KafkaTopic(MAIN_TOPIC, 0);
        firstRetryTopic = new KafkaTopic(FIRST_RETRY_TOPIC, 1);
        secondRetryTopic = new KafkaTopic(SECOND_RETRY_TOPIC, 2);
        thirdRetryTopic = new KafkaTopic(THIRD_RETRY_TOPIC, 3);
        dlqTopic = new KafkaTopic(DLQ_TOPIC, 4);
        kafkaTopicChain = new KafkaTopicChain();
        Stream.of(mainTopic, firstRetryTopic, secondRetryTopic, thirdRetryTopic, dlqTopic).forEach(kafkaTopicChain::add);
    }

    @Test
    public void shouldGetTheFirstTopic() {
        assertEquals(mainTopic, kafkaTopicChain.getFirstTopic());
    }

    @Test
    public void shouldGetTheLastTopic() {
        assertEquals(dlqTopic, kafkaTopicChain.getLastTopic());
    }

    @Test
    public void shouldAddATopicToChain() {
        kafkaTopicChain = new KafkaTopicChain();
        assertEquals(0, kafkaTopicChain.size());
        kafkaTopicChain.add(new KafkaTopic(MAIN_TOPIC, 0));
        assertEquals(1, kafkaTopicChain.size());
    }

    @Test
    public void shouldRemoveATopicFromChain() {
        //full size
        assertEquals(mainTopic, kafkaTopicChain.getFirstTopic());
        assertEquals(firstRetryTopic, kafkaTopicChain.getFirstTopic().getNextTopic());
        assertEquals(thirdRetryTopic, kafkaTopicChain.getLastTopic().getPreviousTopic());
        assertEquals(dlqTopic, kafkaTopicChain.getLastTopic());
        assertEquals(5, kafkaTopicChain.size());
        assertTrue(kafkaTopicChain.contains(mainTopic));
        assertTrue(kafkaTopicChain.contains(firstRetryTopic));
        assertTrue(kafkaTopicChain.contains(secondRetryTopic));
        assertTrue(kafkaTopicChain.contains(thirdRetryTopic));
        assertTrue(kafkaTopicChain.contains(dlqTopic));

        //remove main topic
        kafkaTopicChain.remove(mainTopic);
        assertEquals(firstRetryTopic, kafkaTopicChain.getFirstTopic());
        assertEquals(secondRetryTopic, kafkaTopicChain.getFirstTopic().getNextTopic());
        assertEquals(thirdRetryTopic, kafkaTopicChain.getLastTopic().getPreviousTopic());
        assertEquals(dlqTopic, kafkaTopicChain.getLastTopic());
        assertEquals(4, kafkaTopicChain.size());
        assertFalse(kafkaTopicChain.contains(mainTopic));

        //remove dlq topic
        kafkaTopicChain.remove(dlqTopic);
        assertEquals(firstRetryTopic, kafkaTopicChain.getFirstTopic());
        assertEquals(thirdRetryTopic, kafkaTopicChain.getLastTopic());
        assertEquals(3, kafkaTopicChain.size());
        assertFalse(kafkaTopicChain.contains(dlqTopic));

        //remove second retry topic
        kafkaTopicChain.remove(secondRetryTopic);
        assertEquals(firstRetryTopic, kafkaTopicChain.getFirstTopic());
        assertEquals(firstRetryTopic, kafkaTopicChain.getFirstTopic());
        assertEquals(thirdRetryTopic, kafkaTopicChain.getLastTopic());
        assertEquals(2, kafkaTopicChain.size());
        assertFalse(kafkaTopicChain.contains(secondRetryTopic));

        //remove first retry topic
        kafkaTopicChain.remove(firstRetryTopic);
        assertEquals(thirdRetryTopic, kafkaTopicChain.getFirstTopic());
        assertEquals(thirdRetryTopic, kafkaTopicChain.getLastTopic());
        assertEquals(1, kafkaTopicChain.size());
        assertFalse(kafkaTopicChain.contains(firstRetryTopic));

        //remove third retry topic
        kafkaTopicChain.remove(thirdRetryTopic);
        assertEquals(0, kafkaTopicChain.size());
        assertFalse(kafkaTopicChain.contains(secondRetryTopic));
        assertNull(kafkaTopicChain.getFirstTopic());
        assertNull(kafkaTopicChain.getLastTopic());
    }

    @Test
    public void getKafkaTopicByTopicName() {
        KafkaTopic topic = kafkaTopicChain.getKafkaTopicByName(FIRST_RETRY_TOPIC);
        assertEquals(firstRetryTopic, topic);
    }

    @Test
    public void getKafkaTopicByRetryValue() {
        KafkaTopic topic = kafkaTopicChain.getKafkaTopicByRetryValue(2);
        assertEquals(secondRetryTopic, topic);
    }

    @Test
    public void shouldListKafkaTopicsAsString() {
        String expected =
                "0 : KafkaTopic(topicName=test-topic, retryValue=0)\n" +
                        "1 : KafkaTopic(topicName=first-retry-test-topic, retryValue=1)\n" +
                        "2 : KafkaTopic(topicName=second-retry-test-topic, retryValue=2)\n" +
                        "3 : KafkaTopic(topicName=third-retry-test-topic, retryValue=3)\n" +
                        "4 : KafkaTopic(topicName=dlq-test-topic, retryValue=4)\n";

        assertEquals(expected, kafkaTopicChain.toString());
    }

    @Test
    public void shouldThrowAnErrorWhenCannotFindKafkaTopic() {
        assertThrows(UnregisteredTopicException.class, () -> kafkaTopicChain.getKafkaTopicByName(INVALID_TOPIC_NAME));
        assertThrows(UnregisteredTopicException.class, () -> kafkaTopicChain.getKafkaTopicByRetryValue(1000));
    }
}
