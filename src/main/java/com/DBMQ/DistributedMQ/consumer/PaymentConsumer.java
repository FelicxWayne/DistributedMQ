package com.DBMQ.DistributedMQ.consumer;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.model.OrderEvent;
import com.DBMQ.DistributedMQ.service.AckService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.stereotype.Component;

@Component
public class PaymentConsumer extends ConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    public PaymentConsumer(AckService ackService) {
        super(StreamConstants.ORDER_PLACED, StreamConstants.PAYMENT_GROUP, ackService);
    }

    @Override
    protected void process(OrderEvent event, RecordId recordId) throws Exception {
        log.info("[PaymentConsumer] Processing payment for order: {} (userId: {}, amount: {})", 
                 event.getOrderId(), event.getUserId(), event.getAmount());
    }
}

