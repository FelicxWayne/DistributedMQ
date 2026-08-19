package com.DBMQ.DistributedMQ;

import com.DBMQ.DistributedMQ.repository.IdempotentRecordRepository;
import com.DBMQ.DistributedMQ.service.IdempotencyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class IdempotencyIntegrationTest {

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private IdempotentRecordRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    @Test
    void testIdempotencyFlow() {
        String msgId = "12345-0";
        String group = "test-group";

        // 1. Initially message is not processed
        assertFalse(idempotencyService.isMessageProcessed(msgId, group));

        // 2. Mark message as processed
        idempotencyService.markMessageAsProcessed(msgId, group);
        assertTrue(idempotencyService.isMessageProcessed(msgId, group));

        // 3. Duplicate mark throws constraint violation
        assertThrows(DataIntegrityViolationException.class, () -> {
            idempotencyService.markMessageAsProcessed(msgId, group);
        });
    }
}
