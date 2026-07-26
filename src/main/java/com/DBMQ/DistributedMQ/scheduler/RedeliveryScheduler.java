package com.DBMQ.DistributedMQ.scheduler;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.service.PendingMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
@EnableScheduling
public class RedeliveryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RedeliveryScheduler.class);
    private static final long MAX_PENDING_FETCH = 100;

    private final PendingMessageService pendingMessageService;

    public RedeliveryScheduler(PendingMessageService pendingMessageService) {
        this.pendingMessageService = pendingMessageService;
    }

    /**
     * Periodically queries and logs the Pending Entries List (PEL) for each consumer group.
     * Scheduled to execute every 5 seconds.
     */
    @Scheduled(fixedDelay = 5000)
    public void observePendingMessages() {
        for (String group : StreamConstants.CONSUMER_GROUPS) {
            observeGroupPendingMessages(StreamConstants.ORDER_PLACED, group);
        }
    }

    private void observeGroupPendingMessages(String streamKey, String groupName) {
        try {
            // Retrieve pending messages from the PEL for the group using the constant limit
            List<PendingMessage> pendingMessages = pendingMessageService.getPendingMessages(
                    streamKey,
                    groupName,
                    MAX_PENDING_FETCH
            );

            for (PendingMessage pending : pendingMessages) {
                String messageId = pending.getIdAsString();
                String consumer = pending.getConsumerName();
                Duration idleTime = pending.getElapsedTimeSinceLastDelivery();
                long deliveryCount = pending.getTimesDelivered();

                log.info("\n==============================\n" +
                                "Consumer Group : {}\n" +
                                "Message ID     : {}\n" +
                                "Consumer       : {}\n" +
                                "Idle Time      : {} sec\n" +
                                "Deliveries     : {}\n" +
                                "==============================",
                        groupName,
                        messageId,
                        consumer,
                        idleTime != null ? idleTime.toSeconds() : "unknown",
                        deliveryCount
                );
            }
        } catch (Exception e) {
            log.error("Failed to observe pending messages for group {}: {}", groupName, e.getMessage(), e);
        }
    }
}
