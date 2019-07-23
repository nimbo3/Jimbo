package ir.jimbo.crawler.kafka;

import ir.jimbo.commons.model.Page;
import ir.jimbo.crawler.config.KafkaConfiguration;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class PageProducer {

    private Producer<Long, Page> pageProducer;
    private Producer<Long, String> linkProducer;

    public PageProducer(KafkaConfiguration data) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, data.getProperty("bootstrap.servers"));
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, data.getProperty("client.id"));
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PageSerializer.class.getName());
        pageProducer = new KafkaProducer<>(producerProperties);

        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public void addPageToKafka(String topicName, Page value) {

        ProducerRecord<Long, Page> record = new ProducerRecord<>(topicName, value);
        // use metadata for log
        RecordMetadata metadata;
        try {
            metadata = pageProducer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            // logging here(this happen when getting metadata not sending it)
        }
    }

    public void addLinkToKafka(String topicName, String link) {
        ProducerRecord<Long, String> record = new ProducerRecord<>(topicName, link);
        // use metadata for log
        RecordMetadata metadata;
        try {
            metadata = linkProducer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            // logging here(this happen when getting metadata not sending it)
        }
    }
}
