package com.atreyee.kafkaconsumer.processor;

import com.atreyee.kafkaconsumer.util.Constants;
import com.loader.serializer.avro.BankProduct;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class ProductDataProcessor {

    private Logger log = LoggerFactory.getLogger(ProductDataProcessor.class);

    @Value("${product.producer.topic}")
    private String topic;

    public void processProductDetails(StreamsBuilder builder, Map<String, String> serdeConfig){
        log.debug("processProductDetails called");
        SpecificAvroSerde<BankProduct> bankProductSerde = new SpecificAvroSerde<>();
        bankProductSerde.configure(serdeConfig, false);

        log.debug("saving CustomerDetails in store");
        KTable<String,BankProduct> customerList = builder.table(topic, Materialized.<String,BankProduct, KeyValueStore<Bytes,byte[]>>as(Constants.PRODUCT_COUNT_STORE.getValue()).withKeySerde(Serdes.String()).withValueSerde(bankProductSerde));
    }
}
