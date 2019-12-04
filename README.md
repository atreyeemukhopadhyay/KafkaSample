# KafkaSample
Sample application for  producing data to kafka topic and consuming from the same. 

Kakfa Producer is a springboot application that reads file from a configured location every 24 hours and push the data to kafka topic in Avro format.

Kafka Consumer is a springboot application that reads data from kafka topic.Rest endpoints are exposed to retrieve different details.

Sample inout fie is available in KafkaProducer\src\main\resources\Sample_Input directory.

Kafka details can be configured in application.properties file.

Instructions to run:

To run the KafkaProducer/KafkaConsumer application,

1)mvn clean package. 

2)mvn spring-boot:run or java -jar target\<application>-0.0.1.jar


KSQL is used to filter data in kafka topic. 

//To filer customer data
CREATE STREAM customer_rowcount AS SELECT 1 AS countcol FROM bank-customers;
CREATE STREAM customer-count AS SELECT count(*) FROM customer_rowcount GROUP BY countcol;

//To filter producer data
CREATE STREAM product_rowcount AS SELECT 1 AS countcol FROM bank-products;
CREATE STREAM product-count AS SELECT countcol,count(*) FROM product_rowcount GROUP BY countcol;
