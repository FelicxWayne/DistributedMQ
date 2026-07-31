package com.DBMQ.DistributedMQ.service;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Service
public class DlqService {

    private final RedisTemplate<String, Object> redisTemplate;

    public DlqService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Publishes a message that exceeded the retry limit to the Dead Letter Queue.
     *
     * @param originalRecord       the original message record from the stream
     * @param originalConsumerGroup the consumer group processing the message
     * @param deliveryCount         the total delivery count of the message
     * @return the RecordId of the newly created DLQ record
     */
    public RecordId publishToDlq(MapRecord<String, Object, Object> originalRecord, String originalConsumerGroup, long deliveryCount) {
        Map<Object, Object> dlqPayload = new HashMap<>(originalRecord.getValue());
        
        dlqPayload.put("originalMessageId", originalRecord.getId().getValue());
        dlqPayload.put("originalConsumerGroup", originalConsumerGroup);
        dlqPayload.put("deliveryCount", String.valueOf(deliveryCount));
        dlqPayload.put("movedAt", Instant.now().toString());

        MapRecord<String, Object, Object> dlqRecord = MapRecord.create(
                StreamConstants.DLQ_STREAM,
                dlqPayload
        );

        return redisTemplate.opsForStream().add(dlqRecord);
    }
}
