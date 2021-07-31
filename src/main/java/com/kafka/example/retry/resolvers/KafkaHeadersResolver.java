package com.kafka.example.retry.resolvers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;

import static com.kafka.example.retry.utils.KafkaRetryHeaders.FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER;
import static com.kafka.example.retry.utils.KafkaRetryHeaders.LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;

@Component
public class KafkaHeadersResolver {
    public BiFunction<ConsumerRecord<?, ?>, Exception, Headers> resolveKafkaHeaders() {
        return (record, exception) -> {
            Headers headers = record.headers();
            long attemptTimestamp = currentTimeMillis();
            byte[] attemptTimestampInBytes = valueOf(attemptTimestamp).getBytes(UTF_8);

            if (null == headers.lastHeader(FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER)) {
                headers.add(new RecordHeader(FIRST_PROCESS_ATTEMPT_TIMESTAMP_HEADER, attemptTimestampInBytes));
            }
            if (null != headers.lastHeader(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER)) {
                headers.remove(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER);
            }

            headers.add(new RecordHeader(LAST_RETRY_ATTEMPT_TIMESTAMP_HEADER,
                    attemptTimestampInBytes));
            return headers;
        };
    }
}
