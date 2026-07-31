package com.DBMQ.DistributedMQ.scheduler;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.consumer.RecoveryConsumer;
import com.DBMQ.DistributedMQ.service.AckService;
import com.DBMQ.DistributedMQ.service.ClaimService;
import com.DBMQ.DistributedMQ.service.DlqService;
import com.DBMQ.DistributedMQ.service.PendingMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.RecordId;
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
    private final RecoveryConsumer recoveryConsumer;
    private final DlqService dlqService;
    private final AckService ackService;

    @Value("${mq.visibility-timeout-seconds:30}")
    private long visibilityTimeoutSeconds;

    @Value("${mq.max-delivery-attempts:5}")
    private int maxDeliveryAttempts;

    public RedeliveryScheduler(PendingMessageService pendingMessageService,
                               ClaimService claimService,
                               RecoveryConsumer recoveryConsumer,
                               DlqService dlqService,
                               AckService ackService) {
        this.pendingMessageService = pendingMessageService;
        this.claimService = claimService;
        this.recoveryConsumer = recoveryConsumer;
        this.dlqService = dlqService;
        this.ackService = ackService;
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
                    if (deliveryCount >= maxDeliveryAttempts) {
                        log.warn("Message ID: {} in group {} has exceeded retry limit (delivery count: {} >= max: {}). Moving to DLQ.",
                                messageId, groupName, deliveryCount, maxDeliveryAttempts);
                        MapRecord<String, Object, Object> originalRecord = pendingMessageService.getMessageById(streamKey, messageId);
                        if (originalRecord != null) {
                            dlqService.publishToDlq(originalRecord, groupName, deliveryCount);
                            ackService.acknowledge(streamKey, groupName, RecordId.of(messageId));
                            log.info("Successfully moved message ID: {} in group {} to DLQ", messageId, groupName);
                        } else {
                            log.error("Could not find message payload for ID: {} in stream {}. Cannot move to DLQ.", messageId, streamKey);
                        }
                    } else {
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
                                recoveryConsumer.accept(groupName, record);
                            }
                        } else {
                            log.warn("Failed to claim message ID: {} for group: {}", messageId, groupName);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to observe pending messages for group {}: {}", groupName, e.getMessage(), e);
        }
    }
}
