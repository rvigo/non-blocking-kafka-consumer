package retry.resolvers;

import com.kafka.example.retry.exceptions.RecoverableException;
import com.kafka.example.retry.resolvers.KafkaHeadersResolver;
import com.kafka.example.retry.utils.KafkaRetryHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(SpringExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class KafkaHeadersResolverTest {
    private static final String TOPIC_NAME = "test-topic";
    private static final long OFFSET = 1000L;
    private static final int PARTITION = 1;
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private final KafkaHeadersResolver resolver = new KafkaHeadersResolver();
    private ConsumerRecord<String, String> consumerRecord;
    private final RecoverableException exception = new RecoverableException();

    @BeforeEach
    public void setUp() {
        consumerRecord = new ConsumerRecord<>(TOPIC_NAME, PARTITION, OFFSET, KEY, VALUE);

    }

    @Test
    public void shouldCreateFirstAttemptTimestampHeader() {
        Headers resultHeaders = resolver.resolveKafkaHeaders().apply(consumerRecord, exception);

        assertNotNull(resultHeaders.lastHeader(KafkaRetryHeaders.FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER));
        assertNotNull(resultHeaders.lastHeader(KafkaRetryHeaders.LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER));
    }

    @Test
    public void shouldReplaceLastAttemptTimestampHeader() {
        long now = System.currentTimeMillis() - 10000L;
        consumerRecord.headers().add(KafkaRetryHeaders.FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER, String.valueOf(now).getBytes(StandardCharsets.UTF_8));
        consumerRecord.headers().add(KafkaRetryHeaders.LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER, String.valueOf(now).getBytes(StandardCharsets.UTF_8));

        Headers resultHeaders = resolver.resolveKafkaHeaders().apply(consumerRecord, exception);

        assertNotNull(resultHeaders.lastHeader(KafkaRetryHeaders.FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER));
        assertNotNull(resultHeaders.lastHeader(KafkaRetryHeaders.LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER));
        assertTrue(Long.parseLong(new String(resultHeaders.lastHeader(KafkaRetryHeaders.LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER).value())) > now);
    }
}
