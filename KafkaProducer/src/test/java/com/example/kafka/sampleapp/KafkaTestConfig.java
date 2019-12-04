package com.example.kafka.sampleapp;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@TestConfiguration
public class KafkaTestConfig {

    @Bean
    public EmbeddedKafkaBroker embeddedKafka() {
        return new EmbeddedKafkaBroker(1, true, "test-topic");
    }
}
