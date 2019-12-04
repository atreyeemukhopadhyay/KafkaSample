package com.atreyee.kafkaconsumer.config;

import com.atreyee.kafkaconsumer.processor.CustomerDataProcessor;
import com.atreyee.kafkaconsumer.processor.ProductDataProcessor;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Value("${bootstrap.server}")
    public static String bootstrapServer;

    @Value("${zookeeper.server}")
    public static String zookeeperServer;

    @Value("${customer.producer.topic}")
    public static String producer;

    @Value("${consumer.application}")
    private String applicationName;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Autowired
    ProductDataProcessor productDataProcessor;

    @Autowired
    CustomerDataProcessor customerDataProcessor;

    @Bean
    public Properties streamConfig(){
        final Properties config = new Properties();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"DataConsumer");
        return config;
    }

    @PostConstruct
    public void postConstruct() {
        System.out.println("Started after Spring boot application !");

        dataStream().cleanUp();
        dataStream().start();

    }

    @Bean("dataStream")
    public KafkaStreams dataStream(){
        KafkaStreams kafkaStreams = new KafkaStreams(topology(),streamConfig());
        return kafkaStreams;
    }

    @Bean
    public Topology topology(){
        Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        StreamsBuilder builder = new StreamsBuilder();
        customerDataProcessor.processCustomerDetails(builder,serdeConfig);
        productDataProcessor.processProductDetails(builder,serdeConfig);

        return builder.build();
    }



}
