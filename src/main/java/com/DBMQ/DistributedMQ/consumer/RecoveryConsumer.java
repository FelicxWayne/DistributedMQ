package com.DBMQ.DistributedMQ.consumer;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.model.OrderEvent;
import com.DBMQ.DistributedMQ.service.AckService;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class RecoveryConsumer extends ConsumerBase {

    private final Map<String, ConsumerBase> consumers;

    public RecoveryConsumer(AckService ackService, List<ConsumerBase> consumerList) {
        // Registers with a placeholder/recovery group name; it won't read from Redis directly
        super(StreamConstants.ORDER_PLACED, "recovery-group", ackService);
        this.consumers = consumerList.stream()
                .filter(c -> c != this)
                .collect(Collectors.toMap(ConsumerBase::getConsumerGroupName, c -> c));
    }

    @Override
    protected void process(OrderEvent event, RecordId recordId) throws Exception {
        // RecoveryConsumer delegates handling to specific group consumers,
        // so its own process method remains a no-op.
    }

    /**
     * Accepts a claimed MapRecord along with the consumer group it originally belonged to,
     * and forwards it to the correct consumer's processing flow.
     *
     * @param consumerGroup the consumer group of the message
     * @param record        the claimed Redis Stream record
     */
    public void accept(String consumerGroup, MapRecord<String, Object, Object> record) {
        ConsumerBase consumer = consumers.get(consumerGroup);
        if (consumer != null) {
            log.info("RecoveryConsumer forwarding claimed record {} to {}", record.getId(), consumerGroup);
            consumer.onMessage(record);
        } else {
            log.error("No consumer found for group {}", consumerGroup);
        }
    }
}
