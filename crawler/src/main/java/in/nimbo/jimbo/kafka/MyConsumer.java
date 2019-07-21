package in.nimbo.jimbo.kafka;

import in.nimbo.jimbo.config.KafkaConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer extends Thread{

    Consumer<Long, String> consumer;


    MyConsumer(KafkaConfiguration data) {
        consumer = createConsumer(data.getProperty("hostAndPort"), data.getProperty("groupIdConfig"),
                Integer.parseInt(data.getProperty("maxPollRecord")), data.getProperty("autoCommit"),
                data.getProperty("autoOffsetReset"), data.getProperty("linksTopicName"));
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
        org.apache.kafka.clients.consumer.Consumer<Long, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(linksTopicName));
        return consumer;
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            // Commit the offset of record to broker
            consumer.commitSync();

        }
    }
}
