package com.atreyee.kafkaconsumer.model;

import com.loader.serializer.avro.BankCustomer;

public class BankCustomerModel {
    private String customerId;
    private String customerName;
    private String contactDetails;
    private String products;

    public long getRegistrationTime() {
        return registrationTime;
    }

    public void setRegistrationTime(long registrationTime) {
        this.registrationTime = registrationTime;
    }

    private long registrationTime;

    public BankCustomerModel(String customerId, String customerName, String contactDetails, String products, long registrationTime) {
        this.customerId = customerId;
        this.customerName = customerName;
        this.contactDetails = contactDetails;
        this.products = products;
        this.registrationTime = registrationTime;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getContactDetails() {
        return contactDetails;
    }

    public void setContactDetails(String contactDetails) {
        this.contactDetails = contactDetails;
    }

    public String getProducts() {
        return products;
    }

    public void setProducts(String products) {
        this.products = products;
    }

    public BankCustomer convertToAvroFormat() {
        BankCustomer avroCustomer = new BankCustomer();
        avroCustomer.setCustomerId(this.getCustomerId());
        avroCustomer.setCustomerName(this.getCustomerName());
        avroCustomer.setContactDetails(this.getContactDetails());
        avroCustomer.setProducts(this.getProducts());
        avroCustomer.setRegistrationtime(this.getRegistrationTime());
        return avroCustomer;
    }

    public static BankCustomerModel generateCustomer(BankCustomer customer) {
        return new BankCustomerModel(customer.getCustomerId(), customer.getCustomerName(), customer.getContactDetails(), customer.getProducts(),customer.getRegistrationtime());
    }


}
