package com.example.kafka.serde;

import com.example.kafka.model.BankCustomerBean;
import com.example.kafka.model.BankProductBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class BankProductDeserializer implements Deserializer<BankProductBean> {
    @Override public void close() {
    }
    @Override public void configure(Map<String, ?> arg0, boolean arg1) {
    }
    @Override
    public BankProductBean deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        BankProductBean product = null;
        try {
            product = mapper.readValue(arg1, BankProductBean.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return product;
    }
}