package com.DBMQ.DistributedMQ.consumer;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.model.OrderEvent;
import com.DBMQ.DistributedMQ.service.AckService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.stereotype.Component;

@Component
public class InventoryConsumer extends ConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    public InventoryConsumer(AckService ackService) {
        super(StreamConstants.ORDER_PLACED, StreamConstants.INVENTORY_GROUP, ackService);
    }

    @Override
    protected void process(OrderEvent event, RecordId recordId) throws Exception {
        log.info("[InventoryConsumer] Processing inventory update for order: {} (userId: {})", 
                 event.getOrderId(), event.getUserId());
    }
}

