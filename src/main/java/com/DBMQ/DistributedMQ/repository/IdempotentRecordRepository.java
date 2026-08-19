package com.DBMQ.DistributedMQ.repository;

import com.DBMQ.DistributedMQ.model.IdempotentRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface IdempotentRecordRepository extends JpaRepository<IdempotentRecord, Long> {
    boolean existsByMessageIdAndConsumerGroup(String messageId, String consumerGroup);
}
