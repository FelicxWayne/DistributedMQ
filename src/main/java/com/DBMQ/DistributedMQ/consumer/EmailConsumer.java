package com.DBMQ.DistributedMQ.consumer;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.model.orderEvent;
import com.DBMQ.DistributedMQ.service.AckService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.stereotype.Component;

@Component
public class EmailConsumer extends ConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(EmailConsumer.class);

    public EmailConsumer(AckService ackService) {
        super(StreamConstants.ORDER_PLACED, StreamConstants.EMAIL_GROUP, ackService);
    }

    @Override
    protected void process(orderEvent event, RecordId recordId) throws Exception {
        log.info("[EmailConsumer] Processing email notification for order: {} (userId: {})", 
                 event.getOrderId(), event.getUserId());
    }
}
