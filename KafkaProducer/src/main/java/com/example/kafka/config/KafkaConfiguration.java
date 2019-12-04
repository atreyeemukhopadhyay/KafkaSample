package com.example.kafka.config;

import com.loader.serializer.avro.BankProduct;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.loader.serializer.avro.BankCustomer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Value("${bootstrap.server}")
    private String bootstrapServer;

    @Value("${zookeeper.server}")
    private String zookeeperServer;

    @Value("${customer.producer.topic}")
    private String producer;

    @Value("${producer.application}")
    private String applicationName;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KafkaProducer<String, BankCustomer> customerKafkaProducer(){
        Properties dataloaderConfig = new Properties();

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        dataloaderConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        dataloaderConfig.put(ProducerConfig.ACKS_CONFIG,"all");

        SpecificAvroSerializer<BankCustomer> bankCustomerSerializer = new SpecificAvroSerializer<>();
        bankCustomerSerializer.configure(serdeConfig, false);

        return new KafkaProducer<String, BankCustomer>(dataloaderConfig, Serdes.String().serializer(),bankCustomerSerializer);
    }

    @Bean
    public KafkaProducer<String, BankProduct> productKafkaProducer(){
        Properties dataloaderConfig = new Properties();

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        dataloaderConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        dataloaderConfig.put(ProducerConfig.ACKS_CONFIG,"all");

        SpecificAvroSerializer<BankProduct> bankProductSerializer = new SpecificAvroSerializer<>();
        bankProductSerializer.configure(serdeConfig, false);

        return new KafkaProducer<String, BankProduct>(dataloaderConfig, Serdes.String().serializer(),bankProductSerializer);
    }


}
