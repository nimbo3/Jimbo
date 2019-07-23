package ir.jimbo.crawler;


import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.kafka.LinkConsumer;
import ir.jimbo.crawler.kafka.PageAndLinkProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) {

        RedisConfiguration redisConfiguration;
        try {
            redisConfiguration = new RedisConfiguration();
        } catch (IOException e) {
            LOGGER.error("", e);
            return;
        }

        KafkaConfiguration kafkaConfiguration;
        try {
            kafkaConfiguration = new KafkaConfiguration();
        } catch (IOException e) {
            LOGGER.error("", e);
            return;
        }

        AppConfiguration appConfiguration;
        try {
            appConfiguration = new AppConfiguration();
        } catch (IOException e) {
            LOGGER.error("", e);
            return;
        }

        RedisConnection redisConnection = new RedisConnection(redisConfiguration);
        Parsing parsing = new Parsing(appConfiguration, redisConnection);
        PageAndLinkProducer producer = new PageAndLinkProducer(kafkaConfiguration);
        parsing.init(producer, kafkaConfiguration.getProperty("links.topic.name"),
                kafkaConfiguration.getProperty("pages.topic.name"));
        LinkConsumer consumer = new LinkConsumer(kafkaConfiguration);
        new Cli(producer, kafkaConfiguration.getProperty("links.topic.name")).start();
        producer.addLinkToKafka(kafkaConfiguration.getProperty("links.topic.name"), "https://stackoverflow.com/");
        consumer.startGetLinks(redisConnection, producer, kafkaConfiguration.getProperty("links.topic.name"));

    }
}
