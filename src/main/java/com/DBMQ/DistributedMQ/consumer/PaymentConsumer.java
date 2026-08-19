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
public class PaymentConsumer extends ConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    public PaymentConsumer(AckService ackService, IdempotencyService idempotencyService) {
        super(StreamConstants.ORDER_PLACED, StreamConstants.PAYMENT_GROUP, ackService, idempotencyService);
    }

    @Override
    protected void process(OrderEvent event, RecordId recordId) throws Exception {
        log.info("[PaymentConsumer] Processing payment for order: {} (userId: {}, amount: {})", 
                 event.getOrderId(), event.getUserId(), event.getAmount());
        if (event.getAmount() > 500.0) {
            throw new RuntimeException("Simulated Payment failure - Order rejected");
        }
    }
}

