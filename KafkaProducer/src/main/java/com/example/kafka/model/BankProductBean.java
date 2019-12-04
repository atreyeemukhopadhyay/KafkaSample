package com.example.kafka.model;


import com.loader.serializer.avro.BankProduct;

public class BankProductBean {
    private String productId;
    private String productName;
    private String productType;
    private String productDescription;

    public BankProductBean(ProductBuilder builder) {
        this.productId = builder.productId;
        this.productName = builder.productName;
        this.productType = builder.productType;
        this.productDescription = builder.productDescription;
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

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public BankProduct convertToAvroFormat() {
        BankProduct avroProduct = new BankProduct();
        avroProduct.setProductId(this.getProductId());
        avroProduct.setProductName(this.getProductName());
        avroProduct.setProductType(this.getProductType());
        avroProduct.setProductDescription(this.getProductDescription());

        return avroProduct;
    }

    public static class ProductBuilder {
        private String productId;
        private String productName = "";
        private String productType;
        private String productDescription = "";

        public ProductBuilder(String productId) {
            this.productId = productId;
        }

        public ProductBuilder name(String productName) {
            this.productName = productName;
            return this;
        }

        public ProductBuilder description(String productDescription) {
            this.productDescription = productDescription;
            return this;
        }
        public ProductBuilder type(String productType) {
            this.productType = productType;
            return this;
        }

        public BankProductBean build() {
            BankProductBean product = new BankProductBean(this);
            return product;
        }
    }

    @Override
    public String toString(){
        StringBuilder bean = new StringBuilder();
        bean.append(this.productId+"/").append(this.productName+"/").append(this.productType+"/").append(this.productDescription);
        return bean.toString();
    }
}
