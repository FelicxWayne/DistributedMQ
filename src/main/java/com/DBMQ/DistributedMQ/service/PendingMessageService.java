package com.DBMQ.DistributedMQ.service;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class PendingMessageService {

    private final RedisTemplate<String, Object> redisTemplate;

    public PendingMessageService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Retrieves pending messages for a given stream key and consumer group.
     *
     * @param streamKey the Redis Stream key
     * @param groupName the consumer group name
     * @param limit     the maximum number of pending messages to retrieve
     * @return a list of PendingMessage objects
     */
    public List<PendingMessage> getPendingMessages(String streamKey, String groupName, long limit) {
        PendingMessages pendingMessages = redisTemplate.opsForStream().pending(
                streamKey,
                groupName,
                Range.unbounded(),
                limit
        );
        if (pendingMessages != null) {
            return pendingMessages.stream().toList();
        }
        return Collections.emptyList();
    }

    /**
     * Retrieves a single message from the Redis Stream by its message ID.
     *
     * @param streamKey the Redis Stream key
     * @param messageId the RecordId string
     * @return the MapRecord representing the message, or null if not found
     */
    public MapRecord<String, Object, Object> getMessageById(String streamKey, String messageId) {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().range(
                streamKey,
                Range.of(Range.Bound.inclusive(messageId), Range.Bound.inclusive(messageId))
        );
        if (records != null && !records.isEmpty()) {
            return records.get(0);
        }
        return null;
    }
}
