package com.example.kafka.sampleapp;

import com.example.kafka.model.BankProductBean;
import com.example.kafka.serde.BankCustomerDeserializer;
import com.example.kafka.serde.BankCustomerSerializer;
import com.example.kafka.model.BankCustomerBean;
import com.example.kafka.serde.BankProductDeserializer;
import com.example.kafka.serde.BankProductSerializer;
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
import static org.junit.Assert.*;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.*;

public class KafkaProductTemplateTest {

    private static final String TEMPLATE_TOPIC = "productTemplateTopic";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, TEMPLATE_TOPIC);

    @Test
    public void testTemplate() throws Exception {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka.getEmbeddedKafka());
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", BankProductDeserializer.class);
        DefaultKafkaConsumerFactory<String, BankProductBean> cf =
                new DefaultKafkaConsumerFactory<String, BankProductBean>(consumerProps);
        ContainerProperties containerProperties = new ContainerProperties(TEMPLATE_TOPIC);
        KafkaMessageListenerContainer<String, BankProductBean> container =
                new KafkaMessageListenerContainer<>(cf, containerProperties);
        final BlockingQueue<ConsumerRecord<String, BankProductBean>> records = new LinkedBlockingQueue<>();
        container.setupMessageListener(new MessageListener<String, BankProductBean>() {

            @Override
            public void onMessage(ConsumerRecord<String,BankProductBean> record) {
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
        senderProps.put("value.serializer", BankProductSerializer.class);
        ProducerFactory<String,BankProductBean> pf =
                new DefaultKafkaProducerFactory<String,BankProductBean>(senderProps);
        KafkaTemplate<String,BankProductBean> template = new KafkaTemplate<>(pf);
        template.setDefaultTopic(TEMPLATE_TOPIC);

        BankProductBean bankProduct = new BankProductBean.ProductBuilder("Product 1").description("credit card").build();
        template.sendDefault(0,bankProduct.getProductId(),bankProduct);

        ConsumerRecord<String,BankProductBean> received = records.poll(20, TimeUnit.SECONDS);
        assertThat(received, hasKey("Product 1"));
        assertNotNull(received.value());
        assertEquals(received.value().getClass(),bankProduct.getClass());
        assertEquals(received.value().getProductId(),"Product 1");
        assertEquals(received.value().getProductDescription(),"credit card");
    }
}