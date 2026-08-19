package com.DBMQ.DistributedMQ.consumer;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.model.OrderEvent;
import com.DBMQ.DistributedMQ.service.AckService;
import com.DBMQ.DistributedMQ.service.IdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.stereotype.Component;

@Component
public class EmailConsumer extends ConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(EmailConsumer.class);

    public EmailConsumer(AckService ackService, IdempotencyService idempotencyService) {
        super(StreamConstants.ORDER_PLACED, StreamConstants.EMAIL_GROUP, ackService, idempotencyService);
    }

    @Override
    protected void process(OrderEvent event, RecordId recordId) throws Exception {
        log.info("[EmailConsumer] Processing email notification for order: {} (userId: {})", 
                 event.getOrderId(), event.getUserId());
    }
}
