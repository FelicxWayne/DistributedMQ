package com.DBMQ.DistributedMQ.scheduler;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.service.ClaimService;
import com.DBMQ.DistributedMQ.service.PendingMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
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
    private static final String RECOVERY_CONSUMER = "recovery-consumer";

    private final PendingMessageService pendingMessageService;
    private final ClaimService claimService;

    @Value("${mq.visibility-timeout-seconds:30}")
    private long visibilityTimeoutSeconds;

    public RedeliveryScheduler(PendingMessageService pendingMessageService, ClaimService claimService) {
        this.pendingMessageService = pendingMessageService;
        this.claimService = claimService;
    }

    /**
     * Periodically queries and checks the Pending Entries List (PEL) for each consumer group.
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
                long deliveryCount = pending.getTotalDeliveryCount();

                long idleSeconds = idleTime != null ? idleTime.toSeconds() : 0;
                boolean eligibleForRedelivery = idleSeconds >= visibilityTimeoutSeconds;

                String status = eligibleForRedelivery
                        ? String.format("Eligible for redelivery (idle time %ds >= visibility timeout %ds)", idleSeconds, visibilityTimeoutSeconds)
                        : String.format("Still being processed (idle time %ds < visibility timeout %ds)", idleSeconds, visibilityTimeoutSeconds);

                log.info("\n==============================\n" +
                                "Consumer Group : {}\n" +
                                "Message ID     : {}\n" +
                                "Consumer       : {}\n" +
                                "Idle Time      : {} sec\n" +
                                "Deliveries     : {}\n" +
                                "Status         : {}\n" +
                                "==============================",
                        groupName,
                        messageId,
                        consumer,
                        idleSeconds,
                        deliveryCount,
                        status
                );

                if (eligibleForRedelivery) {
                    List<MapRecord<String, Object, Object>> claimedRecords = claimService.claimMessage(
                            streamKey,
                            groupName,
                            RECOVERY_CONSUMER,
                            messageId,
                            Duration.ofSeconds(visibilityTimeoutSeconds)
                    );
                    if (claimedRecords != null && !claimedRecords.isEmpty()) {
                        for (MapRecord<String, Object, Object> record : claimedRecords) {
                            log.info("Successfully claimed message ID: {} for group: {} (assigned to consumer: {})",
                                    record.getId(), groupName, RECOVERY_CONSUMER);
                        }
                    } else {
                        log.warn("Failed to claim message ID: {} for group: {}", messageId, groupName);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to observe pending messages for group {}: {}", groupName, e.getMessage(), e);
        }
    }
}
