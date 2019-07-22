package ir.jimbo.pageprocessor.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.Page;
import ir.jimbo.pageprocessor.config.KConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KConsumer {
    private static final Logger LOGGER = LogManager.getLogger("HelloWorld");
    private Consumer<Long, String> consumer;
    private long pollDuration;

    KConsumer(KConfig kConfig) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kConfig.getPropertyValue("bootstrap." +
                "servers"));
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, kConfig.getPropertyValue("consumer.group.id"));
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.parseInt(kConfig.getPropertyValue(
                "consumer.max.poll.record")));
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kConfig.getPropertyValue("consumer.auto" +
                ".commit"));
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kConfig.getPropertyValue("consumer.auto." +
                "offset.reset"));
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(kConfig.getPropertyValue("consumer.links.topic.name")));
        pollDuration = Long.parseLong(kConfig.getPropertyValue("consumer.poll.duration"));
    }

    public List<Page> getPagesFromKafka() {
        ArrayList<Page> pages = new ArrayList<>();
        ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(pollDuration));
        // Commit the offset of record to broker
        consumer.commitSync();
        for (ConsumerRecord<Long, String> record : consumerRecords) {
            // for logging we can use methods provide by ConsumerRecord class
            ObjectMapper mapper = new ObjectMapper();
            try {
                pages.add(mapper.readValue(record.value(), Page.class));
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }
        return pages;
    }
}