package com.atreyee.kafkaconsumer.util;

public enum Constants {
    ALL_CUSTOMER_STORE("all_customers"),
    ALL_PRODUCTS_STORE("all_products"),
    CUSTOMER_COUNT_STORE("customer_count"),
    PRODUCT_COUNT_STORE("product_count"),
    NEW_CUSTOMER_STORE("new_customer");

    private String value;

    Constants(String value){
       this.value = value;
    }
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


}
