package com.example.kafka.model;

import com.loader.serializer.avro.BankCustomer;

public class BankCustomerBean {
    private String customerId;
    private String customerName;
    private String contactDetails;
    private String products;
    private long registrationTime;

    public BankCustomerBean() {
    }

    public BankCustomerBean(String customerId, String customerName, String contactDetails, String products, long registrationTime) {
        this.customerId = customerId;
        this.customerName = customerName;
        this.contactDetails = contactDetails;
        this.products = products;
        this.registrationTime = registrationTime;
    }

    public BankCustomerBean(CustomerBuilder builder){
        this.customerId = builder.customerId;
        this.customerName =  builder.customerName;
        this.contactDetails = builder.contactDetails;
        this.products = builder.products;
        this.registrationTime = builder.registrationTime;
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

    public long getRegistrationTime() {
        return registrationTime;
    }

    public void setRegistrationTime(long registrationTime) {
        this.registrationTime = registrationTime;
    }

    public BankCustomer convertToAvroFormat(){
        BankCustomer avroCustomer =  new BankCustomer();
        avroCustomer.setCustomerId(this.getCustomerId());
        avroCustomer.setCustomerName(this.getCustomerName());
        avroCustomer.setContactDetails(this.getContactDetails());
        avroCustomer.setProducts(this.getProducts());
        avroCustomer.setRegistrationtime(this.getRegistrationTime());
        return avroCustomer;
    }
    public static class CustomerBuilder{
        private String customerId;
        private String customerName="";
        private String contactDetails="";
        private String products;
        private long registrationTime = System.currentTimeMillis();

        public CustomerBuilder(String customerId,String products){
            this.customerId = customerId;
            this.products =  products;
        }

        public CustomerBuilder name(String customerName){
            this.customerName = customerName;
            return this;
        }

        public CustomerBuilder contact(String contactDetails){
            this.contactDetails = contactDetails;
            return this;
        }

        public CustomerBuilder registrationTime(long time){
            this.registrationTime = time;
            return this;
        }

        public BankCustomerBean build(){
            BankCustomerBean customer = new BankCustomerBean(this);
            return customer;
        }
    }

    @Override
    public String toString(){
        StringBuilder bean = new StringBuilder();
        bean.append(this.customerId+"/").append(this.customerName+"/").append(this.contactDetails+"/").append(this.products+"/").append(this.registrationTime);
        return bean.toString();
    }
}
