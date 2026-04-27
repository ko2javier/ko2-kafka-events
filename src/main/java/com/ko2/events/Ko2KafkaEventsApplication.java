package com.ko2.events;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;

@SpringBootApplication(exclude = {UserDetailsServiceAutoConfiguration.class})
public class Ko2KafkaEventsApplication {

    public static void main(String[] args) {
        SpringApplication.run(Ko2KafkaEventsApplication.class, args);
    }
}
