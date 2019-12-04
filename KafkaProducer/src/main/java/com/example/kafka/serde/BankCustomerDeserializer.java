package com.example.kafka.serde;

import com.example.kafka.model.BankCustomerBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.loader.serializer.avro.BankCustomer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class BankCustomerDeserializer implements Deserializer<BankCustomerBean> {
    @Override public void close() {
    }
    @Override public void configure(Map<String, ?> arg0, boolean arg1) {
    }
    @Override
    public BankCustomerBean deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        BankCustomerBean customer = null;
        try {
            customer = mapper.readValue(arg1, BankCustomerBean.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return customer;
    }
}