package com.DBMQ.DistributedMQ.initializer;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class ConsumerGroupInitializer implements ApplicationRunner {

    private final RedisTemplate<String, Object> redisTemplate;

    public ConsumerGroupInitializer(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run(ApplicationArguments args) {
        initializeConsumerGroups();
    }

    private void initializeConsumerGroups() {
        StreamConstants.CONSUMER_GROUPS.forEach(this::createConsumerGroup);
    }

    private void createConsumerGroup(String groupName) {

        RedisConnectionFactory factory = redisTemplate.getConnectionFactory();

        if (factory == null) {
            throw new IllegalStateException("RedisConnectionFactory is not available.");
        }

        try {
            factory.getConnection().streamCommands().xGroupCreate(
                    StreamConstants.ORDER_PLACED.getBytes(),
                    groupName,
                    ReadOffset.from("0"),
                    true
            );

            System.out.println("Created Consumer Group : " + groupName);

        }catch (Exception ex) {

            if (ex.getMessage() != null && ex.getMessage().contains("BUSYGROUP")) {
                System.out.println("Consumer Group already exists : " + groupName);
                return;
            }

            if (ex.getCause() != null &&
                    ex.getCause().getMessage() != null &&
                    ex.getCause().getMessage().contains("BUSYGROUP")) {

                System.out.println("Consumer Group already exists : " + groupName);
                return;
            }

            throw ex;
        }
    }
}