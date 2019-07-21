package ir.jimbo.crawler.connection;

import ir.jimbo.crawler.Page;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConnectionKafka {

    Producer<Long, String> producer;
    Consumer<Long, String> consumer;

    ConnectionKafka(String hostAndPort, String clientIdConfig, String groupIdConfig, int maxPollRecord,
                    String autoCommit, String autoOffsetReset, String linksTopicName) {

        producer = createProducer(hostAndPort, clientIdConfig);
        consumer = createConsumer(hostAndPort, groupIdConfig, maxPollRecord, autoCommit, autoOffsetReset, linksTopicName);
    }

    private Producer<Long, String> createProducer(String hostAndPort, String clientIdConfig) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostAndPort);
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdConfig);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProperties);
    }

    private Consumer<Long, String> createConsumer(String hostAndPort, String groupIdConfig, int maxPollRecord,
                                                  String autoCommit, String autoOffsetReset, String linksTopicName) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostAndPort);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecord);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(consumerProperties);
        consumer.subscribe(Collections.singletonList(linksTopicName));
        return consumer;
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

    public void getLinkFromKafka() {
        ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
        // Commit the offset of record to broker
        consumer.commitSync();
    }

}


// etc/host esmashono avaz kon ta motmaen shi server2 kar mikone.