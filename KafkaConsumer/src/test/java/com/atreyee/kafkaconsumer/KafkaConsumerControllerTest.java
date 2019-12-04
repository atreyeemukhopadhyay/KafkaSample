package com.atreyee.kafkaconsumer;

import com.atreyee.kafkaconsumer.controller.DataConsumerController;
import com.atreyee.kafkaconsumer.exception.ConsumerException;
import com.atreyee.kafkaconsumer.model.BankCustomerModel;
import com.atreyee.kafkaconsumer.service.IDataConsumerService;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.server.ResponseStatusException;

import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@WebMvcTest(DataConsumerController.class)
public class KafkaConsumerControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private IDataConsumerService service;


    @Before
    public void before(){
        BankCustomerModel bankCustomer1 =  new BankCustomerModel("test1","customer 1","contact","card details",System.currentTimeMillis());
        BankCustomerModel bankCustomer2 =  new BankCustomerModel("test2","customer 2","contact","card details",System.currentTimeMillis());

        List<BankCustomerModel> newCustomerList = new ArrayList<>();
        newCustomerList.add(bankCustomer1);
        newCustomerList.add(bankCustomer2);

        given(service.customerDetails(anyString())).willReturn(bankCustomer1);
        given(service.existingCustomerCount()).willReturn(new Long(10));
        given(service.existingProductsCount()).willReturn(new Long(2));
        given(service.newlyRegisteredCustomers(anyInt())).willReturn(newCustomerList);

    }
    @Test
    public void testValidCustomerQueryStatus() throws Exception {
        mvc.perform(get("/dataconsumer/customer/c1")
                .contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk());

    }

    @Test
    public void testValidCustomerDetails() throws Exception {
        mvc.perform(get("/dataconsumer/customer/c1")
                .contentType(MediaType.APPLICATION_JSON)).andExpect(jsonPath("customerId", Matchers.is("test1")));
    }

    @Test
    public void testInvalidCustomer() throws Exception {
        given(service.customerDetails("c2")).willThrow(new ConsumerException("Invalid user"));
        mvc.perform(get("/dataconsumer/customer/c2")
                .contentType(MediaType.APPLICATION_JSON)).andExpect(status().isBadRequest());
    }

    //@Test(expected = ResponseStatusException.class)
    public void testInvalidCustomerResponse() throws Exception {
        given(service.customerDetails("c2")).willThrow(new ConsumerException("Invalid user"));
        mvc.perform(get("/dataconsumer/customer/c2")).andReturn();
    }

    @Test
    public void testCustomerCountStatus() throws Exception {
        mvc.perform(get("/dataconsumer/customers")
                .contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk());

    }
    @Test
    public void testCustomerCount() throws Exception {
        mvc.perform(get("/dataconsumer/customers")).equals(new Long(10));


    }

    @Test
    public void testProductCountStatus() throws Exception {
        mvc.perform(get("/dataconsumer/products")
                .contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk());

    }
    @Test
    public void testProductCount() throws Exception {
        mvc.perform(get("/dataconsumer/products")).equals(new Long(2));
    }

    @Test
    public void testNewCustomerQueryStatus() throws Exception {
        mvc.perform(get("/dataconsumer/newcustomer/2")
                .contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk());

    }
    @Test
    public void testNewCustomerQueryResponse() throws Exception {
        mvc.perform(get("/dataconsumer/newcustomer/2")).andExpect(jsonPath("$",hasSize(2)));
    }

    @Test
    public void testNewCustomerQueryResult() throws Exception {
        mvc.perform(get("/dataconsumer/newcustomer/2")).andExpect(jsonPath("$[1].customerId",Matchers.is("test2")));
    }

    //@Test(expected = ResponseStatusException.class)
    public void testIncorrectStoreResponse() throws Exception {
        given(service.newlyRegisteredCustomers(2)).willThrow(new ConsumerException("Invalid store"));
        mvc.perform(get("/dataconsumer/newcustomer/2")).andReturn();
    }

    @Test
    public void testIncorrectStoreStatus() throws Exception {
        given(service.newlyRegisteredCustomers(2)).willThrow(new ConsumerException("Invalid store"));
        mvc.perform(get("/dataconsumer/newcustomer/2")).andExpect(status().isInternalServerError());
    }
}
