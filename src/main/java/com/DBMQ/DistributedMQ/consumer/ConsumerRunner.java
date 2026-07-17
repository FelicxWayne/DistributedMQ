package com.DBMQ.DistributedMQ.consumer;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@EnableScheduling
public class ConsumerRunner {

    private static final Logger log = LoggerFactory.getLogger(ConsumerRunner.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final EmailConsumer emailConsumer;
    private final InventoryConsumer inventoryConsumer;
    private final PaymentConsumer paymentConsumer;

    public ConsumerRunner(RedisTemplate<String, Object> redisTemplate,
                          EmailConsumer emailConsumer,
                          InventoryConsumer inventoryConsumer,
                          PaymentConsumer paymentConsumer) {
        this.redisTemplate = redisTemplate;
        this.emailConsumer = emailConsumer;
        this.inventoryConsumer = inventoryConsumer;
        this.paymentConsumer = paymentConsumer;
    }

    @Scheduled(fixedDelay = 1000)
    public void pollMessages() {
        StreamOperations<String, Object, Object> streamOps = redisTemplate.opsForStream();

        // 1. Poll for Email Consumer Group
        pollForGroup(streamOps, StreamConstants.EMAIL_GROUP, "email-runner-1", emailConsumer);

        // 2. Poll for Inventory Consumer Group
        pollForGroup(streamOps, StreamConstants.INVENTORY_GROUP, "inventory-runner-1", inventoryConsumer);

        // 3. Poll for Payment Consumer Group
        pollForGroup(streamOps, StreamConstants.PAYMENT_GROUP, "payment-runner-1", paymentConsumer);
    }

    private void pollForGroup(StreamOperations<String, Object, Object> streamOps,
                              String groupName,
                              String consumerName,
                              ConsumerBase consumer) {
        try {
            List<MapRecord<String, Object, Object>> records = streamOps.read(
                    Consumer.from(groupName, consumerName),
                    StreamReadOptions.empty(),
                    StreamOffset.create(StreamConstants.ORDER_PLACED, ReadOffset.lastConsumed())
            );

            if (records != null) {
                for (MapRecord<String, Object, Object> record : records) {
                    consumer.onMessage(record);
                }
            }
        } catch (Exception e) {
            log.error("Error polling messages for group {}: {}", groupName, e.getMessage(), e);
        }
    }
}
