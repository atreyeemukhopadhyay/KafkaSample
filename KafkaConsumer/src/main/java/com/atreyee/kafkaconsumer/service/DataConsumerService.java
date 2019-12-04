package com.atreyee.kafkaconsumer.service;

import com.atreyee.kafkaconsumer.exception.ConsumerException;
import com.atreyee.kafkaconsumer.exception.ErrorMessages;
import com.atreyee.kafkaconsumer.model.BankCustomerModel;
import com.atreyee.kafkaconsumer.util.Constants;
import com.loader.serializer.avro.BankCustomer;
import com.loader.serializer.avro.BankProduct;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Service
public class DataConsumerService implements IDataConsumerService {

    private Logger log = LoggerFactory.getLogger(DataConsumerService.class);

    @Autowired
    KafkaStreams dataStreams;

    public BankCustomerModel customerDetails(String customerId) {
        log.info("customerDetails called with {}", customerId);
        String storeName = Constants.ALL_CUSTOMER_STORE.getValue();
        try {
            ReadOnlyKeyValueStore<String, BankCustomer> allCustomerDetails = dataStreams.store(storeName, QueryableStoreTypes.keyValueStore());

            BankCustomer customer = allCustomerDetails.get(customerId);
            if (customer != null) {
                BankCustomerModel customerdetails = new BankCustomerModel(customer.getCustomerId(), customer.getCustomerName(), customer.getContactDetails(), customer.getProducts(), customer.getRegistrationtime());
                log.info("customerDetails from store {}", customer);
                return customerdetails;
            } else {
                log.error(String.format(ErrorMessages.NO_CUSTOMER_FOUND, customerId));
                throw new ConsumerException(String.format(ErrorMessages.NO_CUSTOMER_FOUND, customerId));
            }
        } catch (InvalidStateStoreException ex) {
            log.error(ex.getMessage());
            throw new ConsumerException(String.format(ErrorMessages.STORE_NOT_FOUND, storeName));
        }
    }

    public List<BankCustomerModel> newlyRegisteredCustomers(int numberOfCustomer) {
        String storeName = Constants.NEW_CUSTOMER_STORE.getValue();
        try {
            ReadOnlyWindowStore<String, BankCustomer> newCustomerDetails = dataStreams.store(storeName, QueryableStoreTypes.windowStore());

            List<BankCustomerModel> bankCustomers = new ArrayList<>();
            newCustomerDetails.all().forEachRemaining(keyValue -> {
                bankCustomers.add(BankCustomerModel.generateCustomer(keyValue.value));
            });

            Collections.sort(bankCustomers, new Comparator<BankCustomerModel>() {
                @Override
                public int compare(BankCustomerModel customer1, BankCustomerModel customer2) {
                     if(customer1.getRegistrationTime()>customer2.getRegistrationTime())
                        return 1;
                     else if(customer1.getRegistrationTime()<customer2.getRegistrationTime())
                         return -1;
                     else
                         return 0;
                }
            });
            return bankCustomers;
        } catch (InvalidStateStoreException ex) {
            log.error(ex.getMessage());
            throw new ConsumerException(String.format(ErrorMessages.STORE_NOT_FOUND, storeName));
        }
    }

    public long existingCustomerCount() {
        String storeName = Constants.CUSTOMER_COUNT_STORE.getValue();
        try {
            ReadOnlyKeyValueStore<String, String> customer = dataStreams.store(storeName, QueryableStoreTypes.keyValueStore());
            KeyValueIterator<String, String> iterator = customer.all();
            if (iterator.hasNext()) {
                long count = Long.parseLong(iterator.next().value);
                return count;
            } else {
                return 0;
            }
        } catch (InvalidStateStoreException ex) {
            log.error(ex.getMessage());
            throw new ConsumerException(String.format(ErrorMessages.STORE_NOT_FOUND, storeName));
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new ConsumerException(e.getMessage());
        }
    }

    public long existingProductsCount() {
        String storeName = Constants.PRODUCT_COUNT_STORE.getValue();
        try {
            ReadOnlyKeyValueStore<String, BankProduct> products = dataStreams.store(storeName, QueryableStoreTypes.keyValueStore());
            return products.approximateNumEntries();
        } catch (InvalidStateStoreException ex) {
            log.error(ex.getMessage());
            throw new ConsumerException(String.format(ErrorMessages.STORE_NOT_FOUND, storeName));
        }
    }
}
