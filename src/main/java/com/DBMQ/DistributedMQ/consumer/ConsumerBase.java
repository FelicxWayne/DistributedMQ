package com.DBMQ.DistributedMQ.consumer;

import com.DBMQ.DistributedMQ.model.orderEvent;
import com.DBMQ.DistributedMQ.service.AckService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.stream.StreamListener;

public abstract class ConsumerBase implements StreamListener<String, MapRecord<String, Object, Object>> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final String streamName;
    private final String consumerGroupName;
    private final AckService ackService;

    protected ConsumerBase(String streamName, String consumerGroupName, AckService ackService) {
        this.streamName = streamName;
        this.consumerGroupName = consumerGroupName;
        this.ackService = ackService;
    }

    @Override
    public void onMessage(MapRecord<String, Object, Object> record) {
        try {
            log.info("Received message in group {}: {}", consumerGroupName, record.getId());
            orderEvent event = deserialize(record);
            process(event, record.getId());
            
            // Acknowledge the message upon successful processing
            ackService.acknowledge(streamName, consumerGroupName, record.getId());
            log.info("Acknowledged message {} for group {}", record.getId(), consumerGroupName);
        } catch (Exception e) {
            log.error("Failed to process message {} in group {}: {}", record.getId(), consumerGroupName, e.getMessage(), e);
            // DO NOT acknowledge. The message remains in the PEL (Pending Entry List)
            // The redelivery scheduler will claim and retry it.
        }
    }

    private orderEvent deserialize(MapRecord<String, Object, Object> record) {
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
        return new orderEvent(orderId, userId, amount);
    }

    protected abstract void process(orderEvent event, RecordId recordId) throws Exception;

    public String getStreamName() {
        return streamName;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }
}
