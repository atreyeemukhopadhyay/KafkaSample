package com.example.kafka.producer;

import com.example.kafka.exception.InvalidFileException;
import com.example.kafka.model.BankCustomerBean;
import com.example.kafka.util.FileOperations;
import com.loader.serializer.avro.BankCustomer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

@Service
public class CustomerDataProducer {
    private Logger log = LoggerFactory.getLogger(CustomerDataProducer.class);

    @Value("${customer.producer.topic}")
    private String producerTopic;

    @Value("${customer.file.input.path}")
    private String customerInputPath;

    @Autowired
    KafkaProducer<String, BankCustomer> customerKafkaProducer;


    public BankCustomerBean generateCustomerData(List<String> customerData) {
        String customerId = "";
        String customerName = "";
        String customerDetails = "";
        String products = "";
        long time = System.currentTimeMillis();
        System.out.println("key---"+time);
        for (String line : customerData) {
            String value = line.split(":")[1];
            if (line.contains("Id"))
                customerId = value;
            else if (line.contains("Name"))
                customerName = value;
            else if (line.contains("Details"))
                customerDetails = value;
            else if (line.contains("Products"))
                products = value;
        }
        BankCustomerBean bankCustomer = new BankCustomerBean.CustomerBuilder(customerId, products).name(customerName).contact(customerDetails).registrationTime(System.currentTimeMillis()).build();
        return bankCustomer;
    }

    public void publishCustomerData() {
        try {
            FileOperations.fetchInputFiles(customerInputPath).forEach(file -> {
                try {
                    List<String> inputData = FileOperations.readInputData(file);
                    BankCustomerBean customer = generateCustomerData(inputData);
                    System.out.println(customer.toString());
                    BankCustomerBean testcustomer = new BankCustomerBean.CustomerBuilder("mamu","test").build();
                    customerKafkaProducer.send(new ProducerRecord<>(producerTopic, testcustomer.getCustomerId(), testcustomer.convertToAvroFormat()));
                    RecordMetadata response = customerKafkaProducer.send(new ProducerRecord<>(producerTopic, customer.getCustomerId(), customer.convertToAvroFormat())).get();
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
