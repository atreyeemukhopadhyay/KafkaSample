package com.atreyee.kafkaconsumer.processor;

import com.atreyee.kafkaconsumer.util.Constants;
import com.loader.serializer.avro.BankCustomer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

@Component
public class CustomerDataProcessor {
    private Logger log = LoggerFactory.getLogger(CustomerDataProcessor.class);

    @Value("${customer.producer.topic}")
    private String producerTopic;

    @Value("${customer.count.topic}")
    private String customerCountTopic;

    public void processCustomerDetails(StreamsBuilder builder, Map<String, String> serdeConfig) {
        log.info("processCustomerDetails called");
        SpecificAvroSerde<BankCustomer> bankCustomerSerde = new SpecificAvroSerde<>();
        bankCustomerSerde.configure(serdeConfig, false);

        log.info("saving CustomerDetails in store");
        KTable<String, BankCustomer> customerList = builder.table(producerTopic, Materialized.<String, BankCustomer, KeyValueStore<Bytes, byte[]>>as(Constants.ALL_CUSTOMER_STORE.getValue()).withKeySerde(Serdes.String()).withValueSerde(bankCustomerSerde));

        TimeWindowedKStream<String, BankCustomer> newCustomers = customerList.toStream().groupByKey().windowedBy(TimeWindows.of(Duration.ofHours(24)));
        newCustomers.aggregate(BankCustomer::new,  (key, newValue, aggregate) -> {
            return newValue;
        } ,Materialized.<String, BankCustomer, WindowStore<Bytes, byte[]>>as(Constants.NEW_CUSTOMER_STORE.getValue())
                .withValueSerde(bankCustomerSerde));

        KTable<String,String> customerCountList = builder.table(customerCountTopic, Materialized.<String,String, KeyValueStore<Bytes,byte[]>>as(Constants.CUSTOMER_COUNT_STORE.getValue()).withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
    }
}
