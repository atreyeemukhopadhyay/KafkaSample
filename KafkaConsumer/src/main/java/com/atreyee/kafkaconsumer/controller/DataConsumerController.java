package com.atreyee.kafkaconsumer.controller;

import com.atreyee.kafkaconsumer.exception.ConsumerException;
import com.atreyee.kafkaconsumer.model.BankCustomerModel;
import com.atreyee.kafkaconsumer.service.IDataConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping(value="/dataconsumer")
public class DataConsumerController {

    @Autowired
    private IDataConsumerService consumer;

    @GetMapping(value = "/customers")
    public ResponseEntity<Long> getCustomerCount() {
        try {
            return new ResponseEntity<>(this.consumer.existingCustomerCount(), HttpStatus.OK);
        }catch (ConsumerException ex){
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,ex.getMessage());
        }
    }

    @GetMapping(value = "/products")
    public ResponseEntity<Long> getProductCount() {
        try {
            return new ResponseEntity<>(this.consumer.existingProductsCount(), HttpStatus.OK);
        }catch (ConsumerException ex){
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,ex.getMessage());
        }
    }

    @GetMapping(value = "/customer/{customerId}")
    public ResponseEntity<BankCustomerModel> getCustomerDetails(@PathVariable String customerId) {
        try {
            BankCustomerModel customerData = this.consumer.customerDetails(customerId);
            return new ResponseEntity<>(customerData, HttpStatus.OK);
        }catch(ConsumerException ex){
           throw new ResponseStatusException(HttpStatus.BAD_REQUEST,ex.getMessage());
        }
    }

    @GetMapping(value = "/newcustomer/{noOfCustomer}")
    public ResponseEntity<List<BankCustomerModel>> getCustomerDetails(@PathVariable int noOfCustomer) {
        try {
            List<BankCustomerModel> customers = this.consumer.newlyRegisteredCustomers(noOfCustomer);
            return new ResponseEntity<>(customers, HttpStatus.OK);
        }catch(ConsumerException ex){
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,ex.getMessage());
        }
    }
}
