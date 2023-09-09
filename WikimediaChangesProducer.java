package io.conductor.demos.kafka.wikimedia;


import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties;
        properties=new Properties();
        ((Properties) properties).setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        ((Properties) properties).setProperty("security.protocol","SASL_SSL");
        ((Properties) properties).setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"3QqgThkB7Pwy7fDJxrJeAT\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzUXFnVGhrQjdQd3k3ZkRKeHJKZUFUIiwib3JnYW5pemF0aW9uSWQiOjczOTc1LCJ1c2VySWQiOjg2MDMxLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJjMjUwYmQxYS1mNDM4LTQ3MjUtYmFlNC1jZGEyMDg5ODM3YWYifX0.vrB4xv9DFHIn7YxDN6_hwKWpizrubWTr9VA9322KfgA\";");
        ((Properties) properties).setProperty("sasl.mechanism","PLAIN");
        ((Properties) properties).setProperty("key.serializer", StringSerializer.class.getName());
        ((Properties) properties).setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String,String>producer=new KafkaProducer<>(properties);
        String topic="wikimedia.recentchange";
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url="https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder=new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource=builder.build();
        eventSource.start();
        TimeUnit.MINUTES.sleep(10);


}
}