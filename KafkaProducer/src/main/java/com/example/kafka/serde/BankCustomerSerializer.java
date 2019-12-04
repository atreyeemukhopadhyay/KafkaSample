package com.example.kafka.serde;

import com.example.kafka.model.BankCustomerBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class BankCustomerSerializer implements Serializer<BankCustomerBean> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }
    @Override
    public byte[] serialize(String arg0, BankCustomerBean arg1) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(arg1).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }
    @Override public void close() {
    }
}