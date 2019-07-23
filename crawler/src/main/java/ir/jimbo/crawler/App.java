package ir.jimbo.crawler;


import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.kafka.MyConsumer;
import ir.jimbo.crawler.kafka.MyProducer;

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

        MyProducer producer = new MyProducer(kafkaConfiguration);
        parsing.init(producer, kafkaConfiguration.getProperty("links.topic.name"),
                kafkaConfiguration.getProperty("pages.topic.name"));
        MyConsumer consumer = new MyConsumer(kafkaConfiguration);

        consumer.run(redisConnection, producer);

    }
}
