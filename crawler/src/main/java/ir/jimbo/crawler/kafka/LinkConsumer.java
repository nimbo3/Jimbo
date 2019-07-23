package ir.jimbo.crawler.kafka;

import ir.jimbo.crawler.ProcessLink;
import ir.jimbo.crawler.RedisConnection;
import ir.jimbo.crawler.config.KafkaConfiguration;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class LinkConsumer {
    private static final Logger LOGGER = LogManager.getLogger(LinkConsumer.class);

    private Consumer<Long, String> consumer;
    private long pollDuration;

    public LinkConsumer(KafkaConfiguration data) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, data.getProperty("bootstrap.servers"));
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, data.getProperty("group.id"));
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.parseInt(data.getProperty("max.poll.record")));
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, data.getProperty("auto.commit"));
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, data.getProperty("auto.offset.reset"));
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(data.getProperty("links.topic.name")));
        pollDuration = Long.parseLong(data.getProperty("poll.duration"));
    }

    public void startGetLinks(RedisConnection redis, PageProducer producer, String linksTopicName) {
        while (true) {
            //
            System.err.println("here");
            //
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(pollDuration));
            // Commit the offset of record to broker
            for (ConsumerRecord<Long, String> record : consumerRecords) {
                // for logging we can use methods provide by ConsumerRecord class
                new ProcessLink(record.value()).init(redis, producer).process(linksTopicName);
            }
            consumer.commitSync();
        }
    }
}
