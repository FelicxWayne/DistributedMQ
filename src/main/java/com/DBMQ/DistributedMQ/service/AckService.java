package com.DBMQ.DistributedMQ.service;

import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class AckService {

    private final RedisTemplate<String, Object> redisTemplate;

    public AckService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void acknowledge(String stream, String consumerGroup, RecordId recordId) {
        redisTemplate.opsForStream().acknowledge(
                stream,
                consumerGroup,
                recordId
        );
    }
}