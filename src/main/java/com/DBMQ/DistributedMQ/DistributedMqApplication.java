package com.DBMQ.DistributedMQ;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableScheduling
@SpringBootApplication
public class DistributedMqApplication {

	public static void main(String[] args) {
		SpringApplication.run(DistributedMqApplication.class, args);
	}

}
