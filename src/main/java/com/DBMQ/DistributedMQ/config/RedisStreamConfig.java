package com.DBMQ.DistributedMQ.config;

import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.consumer.EmailConsumer;
import com.DBMQ.DistributedMQ.consumer.InventoryConsumer;
import com.DBMQ.DistributedMQ.consumer.PaymentConsumer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;

import jakarta.annotation.PreDestroy;
import java.time.Duration;

@Configuration
public class RedisStreamConfig implements ApplicationListener<ApplicationReadyEvent> {

    private final RedisConnectionFactory connectionFactory;
    private final RedisTemplate<String, Object> redisTemplate;
    private final EmailConsumer emailConsumer;
    private final InventoryConsumer inventoryConsumer;
    private final PaymentConsumer paymentConsumer;
    
    private StreamMessageListenerContainer<String, MapRecord<String, Object, Object>> container;

    public RedisStreamConfig(RedisConnectionFactory connectionFactory,
                             RedisTemplate<String, Object> redisTemplate,
                             EmailConsumer emailConsumer,
                             InventoryConsumer inventoryConsumer,
                             PaymentConsumer paymentConsumer) {
        this.connectionFactory = connectionFactory;
        this.redisTemplate = redisTemplate;
        this.emailConsumer = emailConsumer;
        this.inventoryConsumer = inventoryConsumer;
        this.paymentConsumer = paymentConsumer;
    }

    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, Object, Object>> streamMessageListenerContainer() {
        StreamMessageListenerContainerOptions<String, MapRecord<String, Object, Object>> containerOptions =
                StreamMessageListenerContainerOptions.builder()
                        .pollTimeout(Duration.ofMillis(100))
                        .keySerializer(redisTemplate.getKeySerializer())
                        .hashKeySerializer(redisTemplate.getHashKeySerializer())
                        .hashValueSerializer(redisTemplate.getHashValueSerializer())
                        .build();

        this.container = StreamMessageListenerContainer.create(connectionFactory, containerOptions);

        String streamKey = StreamConstants.ORDER_PLACED;

        // Register EmailConsumer
        container.receive(
                Consumer.from(StreamConstants.EMAIL_GROUP, "email-consumer-1"),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                emailConsumer
        );

        // Register InventoryConsumer
        container.receive(
                Consumer.from(StreamConstants.INVENTORY_GROUP, "inventory-consumer-1"),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                inventoryConsumer
        );

        // Register PaymentConsumer
        container.receive(
                Consumer.from(StreamConstants.PAYMENT_GROUP, "payment-consumer-1"),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                paymentConsumer
        );

        return container;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        if (this.container != null) {
            this.container.start();
        }
    }

    @PreDestroy
    public void stop() {
        if (this.container != null) {
            this.container.stop();
        }
    }
}
