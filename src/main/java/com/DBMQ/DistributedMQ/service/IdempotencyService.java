package com.DBMQ.DistributedMQ.service;

import com.DBMQ.DistributedMQ.model.IdempotentRecord;
import com.DBMQ.DistributedMQ.repository.IdempotentRecordRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IdempotencyService {

    private final IdempotentRecordRepository repository;

    public IdempotencyService(IdempotentRecordRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public boolean isMessageProcessed(String messageId, String consumerGroup) {
        return repository.existsByMessageIdAndConsumerGroup(messageId, consumerGroup);
    }

    @Transactional
    public void markMessageAsProcessed(String messageId, String consumerGroup) {
        IdempotentRecord record = new IdempotentRecord(messageId, consumerGroup);
        repository.save(record);
    }
}
