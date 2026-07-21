package com.DBMQ.DistributedMQ;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class DistributedMqApplication {

	public static void main(String[] args) {
		SpringApplication.run(DistributedMqApplication.class, args);
	}

}
