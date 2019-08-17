package ir.jimbo.shuffler.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

public class KafkaConfiguration {

    private String linkTopicName;
    private String shuffledLinksTopicName;
    private int pollDuration;
    private String autoOffsetReset;
    private String autoCommit;
    private int maxPollRecord;
    private int maxPollInterval;
    private String groupId;
    private String clientId;
    private String bootstrapServers;

    public KafkaConfiguration() throws IOException {
        Properties properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("configs.properties")));
        initValues(properties);
    }

    private void initValues(Properties properties) {
        pollDuration = Integer.parseInt(properties.getProperty("poll.duration"));
        linkTopicName = properties.getProperty("links.topic.name");
        autoOffsetReset = properties.getProperty("auto.offset.reset");
        autoCommit = properties.getProperty("auto.commit");
        maxPollRecord = Integer.parseInt(properties.getProperty("max.poll.record"));
        groupId = properties.getProperty("group.id");
        clientId = properties.getProperty("client.id");
        bootstrapServers = properties.getProperty("bootstrap.servers");
        maxPollInterval = Integer.parseInt(properties.getProperty("max.poll.interval"));
        shuffledLinksTopicName = properties.getProperty("shuffled.links.topic.name");
    }

    public int getPollDuration() {
        return pollDuration;
    }

    public String getLinkTopicName() {
        return linkTopicName;
    }

    public Producer<Long, String> getLinkProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProperties);
    }

    public Consumer<Long, String> getConsumer() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecord);
        consumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        Consumer<Long, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(linkTopicName));
        return consumer;
    }

    public String getShuffledLinksTopicName() {
        return shuffledLinksTopicName;
    }
}
