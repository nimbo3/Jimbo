package ir.jimbo.crawler;


import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.kafka.LinkConsumer;
import ir.jimbo.crawler.kafka.PageProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException {

        RedisConfiguration redisConfiguration;
        try {
            redisConfiguration = new RedisConfiguration();
        } catch (IOException e) {
            LOGGER.error("", e);
            return;
        }
        RedisConnection redisConnection = new RedisConnection(redisConfiguration);

        AppConfiguration appConfiguration;
        try {
            appConfiguration = new AppConfiguration();
        } catch (IOException e) {
            LOGGER.error("", e);
            return;
        }
        Parsing parsing = new Parsing(appConfiguration, redisConnection);

        KafkaConfiguration kafkaConfiguration;
        try {
            kafkaConfiguration = new KafkaConfiguration();
        } catch (IOException e) {
            LOGGER.error("", e);
            return;
        }

        PageProducer producer = new PageProducer(kafkaConfiguration);
        parsing.init(producer, kafkaConfiguration.getProperty("links.topic.name"),
                kafkaConfiguration.getProperty("pages.topic.name"));
        LinkConsumer consumer = new LinkConsumer(kafkaConfiguration);

        consumer.startGetLinks(redisConnection, producer, kafkaConfiguration.getProperty("links.topic.name"));

    }
}
