package io.conductor.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("i am a kafka producer");
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


        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //to use round-robin partitioner:
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        //properties.setProperty("batch.size","400");


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0;j<10;j++) {
            for (int i = 0; i < 30; i++) {
                // create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world" + i);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes everytime a record is successfully sent or exception is thrown
                        if (e == null) {
                            log.info("Received metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset());
                        } else {
                            log.error("error while producing");
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // this follows sticky partitioner.
        // it sends messages as batch wise into  partitions

        // tell the producer to send all the data and block until done --synchronous
        producer.flush();

        // close the producer
        producer.close();

    }
}
