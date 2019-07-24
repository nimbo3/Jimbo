package ir.jimbo.pageprocessor.config;

import ir.jimbo.commons.model.Page;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class KafkaConfiguration extends Config {
    private static final Logger LOGGER = LogManager.getLogger(HConfig.class);
    private static final String PREFIX = "kafka";

    private static KafkaConfiguration instance;

    static {
        try {
            instance = new KafkaConfiguration();
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }

    private KafkaConfiguration() throws IOException {
        super(PREFIX);
    }

    public Properties getConsumerPageProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getPropertyValue("bootstrap." +
                "servers"));
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, getPropertyValue("consumer.group.id"));
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaPageJsonDeserializer.class);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.parseInt(getPropertyValue(
                "consumer.max.poll.record")));
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, getPropertyValue("consumer.auto" +
                ".commit"));
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, getPropertyValue("consumer.auto." +
                "offset.reset"));
        return consumerProperties;
    }

    public Consumer<Long, Page> getPageConsumer() {
        KafkaConsumer<Long, Page> consumer = new KafkaConsumer<>(getConsumerPageProperties());
        consumer.subscribe(Collections.singletonList(getPropertyValue("consumer.pages.topic.name")));
        return consumer;
    }


    public Properties getLinkProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getPropertyValue("bootstrap.servers"));
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, getPropertyValue("producer.client.id"));
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerProperties;
    }

    public KafkaProducer<Long, String> getLinkProducer() {
        return new KafkaProducer<>(getLinkProducerProperties());
    }

    public static KafkaConfiguration getInstance() {
        return instance;
    }
}
