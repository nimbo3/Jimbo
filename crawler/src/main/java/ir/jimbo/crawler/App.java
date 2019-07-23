package ir.jimbo.crawler;


import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.kafka.LinkConsumer;
import ir.jimbo.crawler.kafka.PageProducer;

import java.io.IOException;

public class App {
    public static void main( String[] args ) {

        RedisConfiguration redisConfiguration;
        try {
            redisConfiguration = new RedisConfiguration();
        } catch (IOException e) {
            return;
        }
        RedisConnection redisConnection = new RedisConnection(redisConfiguration);

        AppConfiguration appConfiguration;
        try {
            appConfiguration = new AppConfiguration();
        } catch (IOException e) {
            return;
        }
        Parsing parsing = new Parsing(appConfiguration, redisConnection);

        KafkaConfiguration kafkaConfiguration;
        try {
            kafkaConfiguration = new KafkaConfiguration();
        } catch (IOException e) {
            return;
        }

        PageProducer producer = new PageProducer(kafkaConfiguration);
        parsing.init(producer, kafkaConfiguration.getProperty("links.topic.name"),
                kafkaConfiguration.getProperty("pages.topic.name"));
        LinkConsumer consumer = new LinkConsumer(kafkaConfiguration);

        consumer.startGetLinks(redisConnection, producer, kafkaConfiguration.getProperty("links.topic.name"));

    }
}
