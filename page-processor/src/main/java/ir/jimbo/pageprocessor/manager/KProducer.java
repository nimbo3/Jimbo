package ir.jimbo.pageprocessor.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import ir.jimbo.commons.model.TitleAndLink;
import ir.jimbo.pageprocessor.config.KConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KProducer {
    private static final Logger LOGGER = LogManager.getLogger(KProducer.class);
    private Producer<Long, String> producer;
    private String topic;

    KProducer(KConfig kConfig, String topic) {
        this.topic = topic;
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kConfig.getPropertyValue("bootstrap.servers"));
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, kConfig.getPropertyValue("producer.client.id"));
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProperties);
    }

    public void addLinkToKafka(TitleAndLink titleAndLink) {
        ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String titleAndLinkJson = null; // ?/
        try {
            titleAndLinkJson = objectWriter.writeValueAsString(titleAndLink);
        } catch (JsonProcessingException e) {
            LOGGER.error("", e);
        }
        ProducerRecord<Long, String> record = new ProducerRecord<>(topic, titleAndLinkJson);
        // use metadata for log
        RecordMetadata metadata;
        try {
            metadata = producer.send(record).get();
            LOGGER.info(metadata);
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("", e);
            Thread.currentThread().interrupt();
        }
    }
}