package com.DBMQ.DistributedMQ.initializer;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class ConsumerGroupInitializer implements ApplicationRunner {

    private final RedisTemplate<String,Object> redisTemplate;

    public ConsumerGroupInitializer(RedisTemplate<String,Object> redisTemplate){
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception{

    }
}
