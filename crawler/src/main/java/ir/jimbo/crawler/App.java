package ir.jimbo.crawler;


import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.kafka.MyConsumer;
import ir.jimbo.crawler.kafka.MyProducer;

import java.io.IOException;

public class App {
    public static void main( String[] args ) {

        RedisConfiguration redisConfiguration = null;
        try {
            redisConfiguration = new RedisConfiguration();
        } catch (IOException e) {
            e.printStackTrace();
        }
        RedisConnection redisConnection = new RedisConnection(redisConfiguration);

        KafkaConfiguration kafkaConfiguration = null;
        try {
            kafkaConfiguration = new KafkaConfiguration();
        } catch (IOException e) {
            e.printStackTrace();
        }
        MyProducer producer = new MyProducer(kafkaConfiguration);
        MyConsumer consumer = new MyConsumer(kafkaConfiguration);
    }
}
