package com.DBMQ.DistributedMQ.service;

import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Service
public class ClaimService {

    private final RedisTemplate<String, Object> redisTemplate;

    public ClaimService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public List<MapRecord<String, Object, Object>> claimMessage(
            String streamKey,
            String groupName,
            String consumerName,
            String messageId,
            Duration minIdleTime
    ) {
        return redisTemplate.opsForStream().claim(
                streamKey,
                groupName,
                consumerName,
                minIdleTime,
                RecordId.of(messageId)
        );
    }
}
