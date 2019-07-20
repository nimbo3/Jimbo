package in.nimbo.jimbo.Connection;

import in.nimbo.jimbo.Page;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConnectionKafka {

    Producer<Long, Page> producer;

    ConnectionKafka(String host, int port, String clientIdConfig) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdConfig);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public void addPageToKafka(String topicName, Page value) {
        ProducerRecord<Long, Page> record = new ProducerRecord<>(topicName,
                value);
        // use metadata for log
        RecordMetadata metadata;
        try {
            metadata = producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            // logging here(this happen when getting metadata not sending it)
        }
    }

}
