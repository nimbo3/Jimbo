package ir.jimbo.crawler.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import ir.jimbo.commons.model.Page;
import ir.jimbo.commons.model.TitleAndLink;
import ir.jimbo.crawler.config.KafkaConfiguration;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {

    Producer<Long, String> producer;

    public MyProducer(KafkaConfiguration data) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, data.getProperty("bootstrap.servers"));
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, data.getProperty("client.id"));
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProperties);
    }

    public void addPageToKafka(String topicName, Page value) {

        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String pageJson = null; // ?/
        try {
            pageJson = ow.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            // log
        }

        ProducerRecord<Long, String> record = new ProducerRecord<>(topicName, pageJson);
        // use metadata for log
        RecordMetadata metadata;
        try {
            metadata = producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            // logging here(this happen when getting metadata not sending it)
        }
    }

    public void addLinkToKafka(String topicName, TitleAndLink titleAndLink) {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String titleAndLinkJson = null; // ?/
        try {
            titleAndLinkJson = ow.writeValueAsString(titleAndLink);
        } catch (JsonProcessingException e) {
            // log
        }
        ProducerRecord<Long, String> record = new ProducerRecord<>(topicName, titleAndLinkJson);
        // use metadata for log
        RecordMetadata metadata;
        try {
            metadata = producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            // logging here(this happen when getting metadata not sending it)
        }
    }
}
