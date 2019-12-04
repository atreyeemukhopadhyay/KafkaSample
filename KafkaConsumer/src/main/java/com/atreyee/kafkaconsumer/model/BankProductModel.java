package com.atreyee.kafkaconsumer.model;

import com.loader.serializer.avro.BankProduct;

public class BankProductModel {
    private String productId;
    private String productName;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductType() {
        return productType;
    }

    public void setProductType(String productType) {
        this.productType = productType;
    }

    public String getProductDescription() {
        return productDescription;
    }

    public void setProductDescription(String productDescription) {
        this.productDescription = productDescription;
    }

    private String productType;
    private String productDescription;

    public BankProductModel(String productId, String productName, String productType, String productDescription) {
        this.productId = productId;
        this.productName = productName;
        this.productType = productType;
        this.productDescription = productDescription;
    }

    public BankProduct convertToAvroFormat() {
        BankProduct avroProduct = new BankProduct();
        avroProduct.setProductId(this.getProductId());
        avroProduct.setProductName(this.getProductName());
        avroProduct.setProductType(this.getProductType());
        avroProduct.setProductDescription(this.getProductDescription());
        return avroProduct;
    }
}
