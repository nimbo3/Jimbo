package ir.jimbo.crawler;

import ir.jimbo.crawler.config.KafkaConfiguration;
import org.apache.kafka.clients.consumer.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

public class LinkConsumer {

    private Logger logger = LogManager.getLogger(this.getClass());
    private long pollDuration;
    private KafkaConfiguration kafkaConfiguration;

    public LinkConsumer(KafkaConfiguration kafkaConfiguration) {
        pollDuration = Long.parseLong(kafkaConfiguration.getProperty("poll.duration"));
        this.kafkaConfiguration = kafkaConfiguration;
    }

    public void startGetLinks(CacheService redis) {
        boolean repeat = true;
        Consumer<Long, String> consumer = kafkaConfiguration.getConsumer();
        while (repeat) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(pollDuration));
            // Commit the offset of record to broker

            logger.info("get link from kafka numbers taken : " + consumerRecords.count());

            for (ConsumerRecord<Long, String> record : consumerRecords) {
                logger.debug("the link readed from kafka : " + record.value());
                // for logging we can use methods provide by ConsumerRecord class
                new ProcessLink(record.value(), redis, kafkaConfiguration).process();
            }
//            consumer.commitSync();
        }
    }
}
