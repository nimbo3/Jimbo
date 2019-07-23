package ir.jimbo.crawler;


import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);
    static Thread[] parserThreads;
    static ArrayBlockingQueue<String> urlToParseQueue;

    public static void main(String[] args) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

        }));

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

        CacheService cacheService = new CacheService(redisConfiguration);
        Parser parser = new Parser(appConfiguration, cacheService, kafkaConfiguration);
        new Cli(kafkaConfiguration).start();
        new LinkConsumer(kafkaConfiguration).startGetLinks(cacheService);
    }
}
