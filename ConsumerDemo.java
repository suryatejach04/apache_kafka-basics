package io.conductor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args)
    {

        String groupId="my-java-application";
        String topic = "demo_java";

        log.info("i am a kafka consumer");
        // create producer properties
        Properties properties = new Properties();

        // to connect local host
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //to connect with conductor
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"UxKNMhEWs9nJbWVidjdOD\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJVeEtOTWhFV3M5bkpiV1ZpZGpkT0QiLCJvcmdhbml6YXRpb25JZCI6NzQzOTYsInVzZXJJZCI6ODY1NDUsImZvckV4cGlyYXRpb25DaGVjayI6IjBhZWYxMzI2LTVhMTQtNDc1NC1iYjllLTgxYjlhMjVjMWIxYyJ9fQ.SBRmKaBNtW_yl8dIEEM0xTnmCAGMY3bYAnRYvBYHcJI\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

//        security.protocol=SASL_SSL
//        sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="UxKNMhEWs9nJbWVidjdOD" password="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJVeEtOTWhFV3M5bkpiV1ZpZGpkT0QiLCJvcmdhbml6YXRpb25JZCI6NzQzOTYsInVzZXJJZCI6ODY1NDUsImZvckV4cGlyYXRpb25DaGVjayI6IjBhZWYxMzI2LTVhMTQtNDc1NC1iYjllLTgxYjlhMjVjMWIxYyJ9fQ.SBRmKaBNtW_yl8dIEEM0xTnmCAGMY3bYAnRYvBYHcJI";
//        sasl.mechanism=PLAIN

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);

        properties.setProperty("auto.offset.rest","earliest");

        //create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while(true)
        {
            log.info("polling");

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String,String> record : records){
                log.info("key: "+record.key() + "value: "+ record.value());
                log.info("partition: "+record.partition() + "offset: "+ record.offset());
            }
        }

           }
}
