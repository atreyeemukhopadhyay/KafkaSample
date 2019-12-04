package com.atreyee.kafkaconsumer.service;

import com.atreyee.kafkaconsumer.model.BankCustomerModel;
import com.loader.serializer.avro.BankCustomer;

import java.util.List;

public interface IDataConsumerService {

    BankCustomerModel customerDetails(String customerId);

    List<BankCustomerModel> newlyRegisteredCustomers(int numberOfCustomer);

    long existingCustomerCount();

    long existingProductsCount();
}
