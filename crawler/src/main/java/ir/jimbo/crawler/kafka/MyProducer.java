package ir.jimbo.crawler.kafka;

import in.nimbo.jimbo.Page;
import ir.jimbo.crawler.config.KafkaConfiguration;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {

    Producer<Long, String> producer;


    MyProducer(KafkaConfiguration data) {
        producer = createProducer(data.getProperty("hostAndPort"), data.getProperty("clientIdConfig"));
    }

    private Producer<Long, String> createProducer(String hostAndPort, String clientIdConfig) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostAndPort);
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdConfig);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProperties);
    }

    public void addPageToKafka(String topicName, Page value) {
        ProducerRecord<Long, String> record = new ProducerRecord<>(topicName, value.toString());
        // use metadata for log
        RecordMetadata metadata;
        try {
            metadata = producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            // logging here(this happen when getting metadata not sending it)
        }
    }

}
