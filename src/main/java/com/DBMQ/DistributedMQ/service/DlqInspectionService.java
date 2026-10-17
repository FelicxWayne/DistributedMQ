    package com.DBMQ.DistributedMQ.service;

    import com.DBMQ.DistributedMQ.constants.StreamConstants;
    import org.springframework.data.domain.Range;
    import org.springframework.data.redis.connection.Limit;
    import org.springframework.data.redis.connection.stream.MapRecord;
    import org.springframework.data.redis.core.RedisTemplate;
    import org.springframework.stereotype.Service;

    import java.util.Collections;
    import java.util.List;

    @Service
    public class DlqInspectionService {

        private final RedisTemplate<String, Object> redisTemplate;

        private static final int MAX_LIMIT = 1000;

        public DlqInspectionService(RedisTemplate<String, Object> redisTemplate) {
            this.redisTemplate = redisTemplate;
        }

        /**
         * Reads DLQ messages up to a specified limit.
         * Does NOT remove or acknowledge messages.
         *
         * @param limit the maximum number of DLQ messages to read
         * @return a list of DLQ messages represented as MapRecord
         */
        public List<MapRecord<String, Object, Object>> readDlqMessages(int limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("Limit must be greater than zero.");
            }
            if (limit > MAX_LIMIT) {
                throw new IllegalArgumentException("Limit cannot exceed " + MAX_LIMIT + ".");
            }
            try {
                List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().range(
                        StreamConstants.DLQ_STREAM,
                        Range.unbounded(),
                        Limit.limit()
                );
                return records != null ? records : Collections.emptyList();
            } catch (Exception e) {
                throw new RuntimeException("Failed to read from DLQ Stream: " + e.getMessage(), e);
            }
        }

        /**
         * Retrieves a single DLQ message from the Redis Stream by its message ID.
         *
         * @param messageId the RecordId string
         * @return the MapRecord representing the DLQ message, or null if not found
         */
        public MapRecord<String, Object, Object> getDlqMessageById(String messageId) {
            try {
                List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().range(
                        StreamConstants.DLQ_STREAM,
                        Range.of(Range.Bound.inclusive(messageId), Range.Bound.inclusive(messageId))
                );
                if (records != null && !records.isEmpty()) {
                    return records.get(0);
                }
                return null;
            } catch (Exception e) {
                throw new RuntimeException("Failed to retrieve DLQ message with ID " + messageId + ": " + e.getMessage(), e);
            }
        }
    }
