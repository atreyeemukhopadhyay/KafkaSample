package com.example.kafka.sampleapp;

import com.example.kafka.serde.BankCustomerDeserializer;
import com.example.kafka.serde.BankCustomerSerializer;
import com.example.kafka.model.BankCustomerBean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.*;

public class KafkaCustomerTemplateTests {

    private static final String TEMPLATE_TOPIC = "customerTemplateTopic";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, TEMPLATE_TOPIC);

    @Test
    public void testTemplate() throws Exception {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka.getEmbeddedKafka());
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", BankCustomerDeserializer.class);
        DefaultKafkaConsumerFactory<String, BankCustomerBean> cf =
                new DefaultKafkaConsumerFactory<String, BankCustomerBean>(consumerProps);
        ContainerProperties containerProperties = new ContainerProperties(TEMPLATE_TOPIC);
        KafkaMessageListenerContainer<String, BankCustomerBean> container =
                new KafkaMessageListenerContainer<>(cf, containerProperties);
        final BlockingQueue<ConsumerRecord<String, BankCustomerBean>> records = new LinkedBlockingQueue<>();
        container.setupMessageListener(new MessageListener<String, BankCustomerBean>() {

            @Override
            public void onMessage(ConsumerRecord<String,BankCustomerBean> record) {
                System.out.println(record);
                records.add(record);
            }

        });
        container.setBeanName("templateTests");
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());

        Map<String, Object> senderProps =
                KafkaTestUtils.senderProps(embeddedKafka.getEmbeddedKafka().getBrokersAsString());
        senderProps.put("key.serializer", StringSerializer.class);
        senderProps.put("value.serializer", BankCustomerSerializer.class);
        ProducerFactory<String,BankCustomerBean> pf =
                new DefaultKafkaProducerFactory<String,BankCustomerBean>(senderProps);
        KafkaTemplate<String,BankCustomerBean> template = new KafkaTemplate<>(pf);
        template.setDefaultTopic(TEMPLATE_TOPIC);

        BankCustomerBean bankCustomer = new BankCustomerBean.CustomerBuilder("customer 1","Account").registrationTime(System.currentTimeMillis()).build();
        template.sendDefault(0,bankCustomer.getCustomerId(),bankCustomer);

        ConsumerRecord<String,BankCustomerBean> received = records.poll(20, TimeUnit.SECONDS);
        assertThat(received, hasKey("customer 1"));
        assertNotNull(received.value());
        assertEquals(received.value().getClass(),bankCustomer.getClass());
        assertEquals(received.value().getCustomerId(),"customer 1");
        assertEquals(received.value().getProducts(),"Account");
    }

}