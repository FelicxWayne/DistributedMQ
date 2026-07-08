package com.DBMQ.DistributedMQ.producer;
import com.DBMQ.DistributedMQ.constants.StreamConstants;
import com.DBMQ.DistributedMQ.model.orderEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping("/api/messages")

public class MessageProducerController {
    @Autowired
    private RedisTemplate<String,Object> redisTemplate;

    private static final String TOPIC = StreamConstants.ORDER_PLACED;

    @PostMapping("/publish")
    public String publish(@RequestBody orderEvent event){

        Map<String,String> messageBody = new HashMap<>();
        messageBody.put("orderId", event.getOrderId());
        messageBody.put("userId", event.getUserId());
        messageBody.put("amount", String.valueOf(event.getAmount()));

        MapRecord<String, String, String> record = StreamRecords
                .newRecord()
                .ofMap(messageBody)
                .withStreamKey(TOPIC);

        redisTemplate.opsForStream().add(record);

        return "Message published to topic: " + TOPIC
                + " | orderId: " + event.getOrderId();
    }
}
