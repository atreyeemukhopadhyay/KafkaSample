package com.example.kafka.producer;

import com.example.kafka.exception.InvalidFileException;
import com.example.kafka.model.BankProductBean;
import com.example.kafka.util.FileOperations;
import com.loader.serializer.avro.BankProduct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

@Service
public class ProductDataGenerator {

    private Logger log = LoggerFactory.getLogger(ProductDataGenerator.class);

    @Value("${product.producer.topic}")
    private String producerTopic;

    @Value("${product.file.input.path}")
    private String productInputPath;

    @Autowired
    KafkaProducer<String, BankProduct> productKafkaProducer;


    public BankProductBean generateProductData(List<String> productData) {
        String productId = "";
        String productName = "";
        String productType = "";
        String productDescription = "";

        for (String line : productData) {
            String value = line.split(":")[1];
            if (line.contains("Id"))
                productId = value;
            else if (line.contains("Name"))
                productName = value;
            else if (line.contains("Type"))
                productType = value;
            else if (line.contains("Description"))
                productDescription = value;
        }
        BankProductBean bankProduct = new BankProductBean.ProductBuilder(productId).name(productName).type(productType).description(productDescription).build();
        return bankProduct;
    }

    public void publishProductData() {
        try {
            FileOperations.fetchInputFiles(productInputPath).forEach(path -> {
                System.out.println(path.getFileName());
                try {
                    List<String> inputData = FileOperations.readInputData(path);
                    BankProductBean product = generateProductData(inputData);
                    System.out.println(product.toString());
                    RecordMetadata response = productKafkaProducer.send(new ProducerRecord<>(producerTopic, product.getProductId(), product.convertToAvroFormat())).get();
                    log.info("Response : {}", response.toString());
                } catch (InvalidFileException e) {
                    log.error("Error while publishing customer data {} ", e.getMessage());
                    log.debug(e.getDetailedMessage());
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                } catch (ExecutionException e) {
                    log.error(e.getMessage());
                }
            });
        } catch (InvalidFileException ex) {
            log.error("Error while publishing customer data {} ", ex.getMessage());
            log.debug(ex.getDetailedMessage());
        }

    }

}
