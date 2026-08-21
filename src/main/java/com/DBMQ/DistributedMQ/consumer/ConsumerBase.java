package com.DBMQ.DistributedMQ.consumer;

import com.DBMQ.DistributedMQ.model.OrderEvent;
import com.DBMQ.DistributedMQ.service.AckService;
import com.DBMQ.DistributedMQ.service.IdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.dao.DataIntegrityViolationException;

public abstract class ConsumerBase implements StreamListener<String, MapRecord<String, Object, Object>> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final String streamName;
    private final String consumerGroupName;
    private final AckService ackService;
    private final IdempotencyService idempotencyService;

    protected ConsumerBase(String streamName, String consumerGroupName, AckService ackService, IdempotencyService idempotencyService) {
        this.streamName = streamName;
        this.consumerGroupName = consumerGroupName;
        this.ackService = ackService;
        this.idempotencyService = idempotencyService;
    }

    @Override
    public void onMessage(MapRecord<String, Object, Object> record) {
        String messageId = record.getId().getValue();
        try {
            log.info("Received message in group {}: {}", consumerGroupName, record.getId());
            
            // Check if message is already successfully processed for this group
            if (idempotencyService.isMessageProcessed(messageId, consumerGroupName)) {
                log.info("Duplicate message detected for group {}: {}. Skipping business logic and acknowledging.", consumerGroupName, messageId);
                ackService.acknowledge(streamName, consumerGroupName, record.getId());
                return;
            }

            OrderEvent event = deserialize(record);
            process(event, record.getId());
            
            // Record message as successfully processed
            try {
                idempotencyService.markMessageAsProcessed(messageId, consumerGroupName);
            } catch (DataIntegrityViolationException e) {
                // A unique constraint violation indicates another consumer instance in this group saved the record first.
                // The business logic has already executed in this thread, but we handle the duplicate record insertion gracefully
                // and acknowledge the message in Redis to clear it from the PEL.
                log.warn("Unique constraint violation for message {} in group {}. A duplicate record insertion was prevented (duplicate processing occurred). Acknowledging Redis message to clean up.", messageId, consumerGroupName);
                ackService.acknowledge(streamName, consumerGroupName, record.getId());
                return;
            }
            
            // Acknowledge the message upon successful processing
            ackService.acknowledge(streamName, consumerGroupName, record.getId());
            log.info("Acknowledged message {} for group {}", record.getId(), consumerGroupName);
        } catch (Exception e) {
            log.error("Failed to process message {} in group {}: {}", record.getId(), consumerGroupName, e.getMessage(), e);
            // it is NOT acknowledging the message so that the message remains in the PEL
            // The redelivery scheduler will claim and retry it.
        }
    }

    private OrderEvent deserialize(MapRecord<String, Object, Object> record) {
        Object orderIdObj = record.getValue().get("orderId");
        Object userIdObj = record.getValue().get("userId");
        Object amountObj = record.getValue().get("amount");

        String orderId = orderIdObj != null ? orderIdObj.toString() : null;
        String userId = userIdObj != null ? userIdObj.toString() : null;
        double amount = 0.0;
        if (amountObj != null) {
            try {
                amount = Double.parseDouble(amountObj.toString());
            } catch (NumberFormatException e) {
                log.warn("Invalid amount field in record: {}", amountObj);
            }
        }
        return new OrderEvent(orderId, userId, amount);
    }

    protected abstract void process(OrderEvent event, RecordId recordId) throws Exception;

    public String getStreamName() {
        return streamName;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }
}
