package com.example.kafka.producer;

import com.example.kafka.model.BankCustomerBean;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.List;

@Service
public class KafkaPublisher {
    Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    @Autowired
    CustomerDataProducer customerDataProducer;

    @Autowired
    ProductDataGenerator productDataGenerator;

    @Scheduled(cron = "0 0 0 * * ?")
    public void produceStreams() {
        customerDataProducer.publishCustomerData();
        productDataGenerator.publishProductData();
        log.info("data successfully published to topic");

    }


}
