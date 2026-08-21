package com.DBMQ.DistributedMQ.service;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class DlqReplayService {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final DlqInspectionService dlqInspectionService;

    public DlqReplayService(RedisTemplate<String, Object> redisTemplate, DlqInspectionService dlqInspectionService) {
        this.redisTemplate = redisTemplate;
        this.dlqInspectionService = dlqInspectionService;
    }

    /**
     * Replays the selected DLQ message identified by its Redis Stream message ID.
     *
     * @param dlqMessageId the Redis Stream RecordId of the DLQ message
     * @return the RecordId of the new message in the original stream
     */
    public RecordId replayMessage(String dlqMessageId) {
        // 1. Fetch DLQ message
        MapRecord<String, Object, Object> dlqRecord = dlqInspectionService.getDlqMessageById(dlqMessageId);
        if (dlqRecord == null) {
            throw new IllegalArgumentException("DLQ message with ID " + dlqMessageId + " not found.");
        }

        // 2. Extract original payload and remove DLQ-specific metadata
        Map<Object, Object> payload = new HashMap<>(dlqRecord.getValue());
        payload.remove("originalMessageId");
        payload.remove("originalConsumerGroup");
        payload.remove("deliveryCount");
        payload.remove("movedAt");

        // 3. Publish only original business payload back to the original Redis Stream
        // (ORDER_PLACED)
        MapRecord<String, Object, Object> replayRecord = StreamRecords
                .newRecord()
                .ofMap(payload)
                .withStreamKey(StreamConstants.ORDER_PLACED);

        try {
            RecordId newRecordId = redisTemplate.opsForStream().add(replayRecord);
            if (newRecordId == null) {
                throw new IllegalStateException("Failed to replay DLQ message: Redis Stream add operation returned null RecordId.");
            }
            log.info("Successfully replayed message. Original DLQ ID: {}, New Stream RecordId: {}", dlqMessageId,
                    newRecordId.getValue());
            return newRecordId;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to replay DLQ message to original stream: " + e.getMessage(), e);
        }
    }
}
