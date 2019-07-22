package ir.jimbo.crawler.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.TitleAndLink;
import ir.jimbo.crawler.ProcessLink;
import ir.jimbo.crawler.config.KafkaConfiguration;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer extends Thread{

    Consumer<Long, String> consumer;
    long pollDuration;

    MyConsumer(KafkaConfiguration data) {
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

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(pollDuration));
            // Commit the offset of record to broker
            consumer.commitSync();
            for (ConsumerRecord<Long, String> record : consumerRecords) {
                // for logging we can use methods provide by ConsumerRecord class
                TitleAndLink titleAndLink = null;
                ObjectMapper mapper = new ObjectMapper();
                try {
                    titleAndLink = mapper.readValue(record.value(), TitleAndLink.class);
                } catch (IOException e) {
                    // logging here
                }
                new ProcessLink(titleAndLink.getTitle(), titleAndLink.getUrl()).start();
            }
        }
    }
}
