package ir.jimbo.crawler;


import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.thread.PageParserThread;
import org.apache.logging.log4j.LogManager;
import ir.jimbo.crawler.service.CacheService;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);
    static Thread[] parserThreads;
    static ArrayBlockingQueue<String> urlToParseQueue;
    static Thread[] consumerThreads;
    private static int consumerThreadSize;

    public static void main(String[] args) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Thread t : consumerThreads) {
                t.interrupt();
            }
            while (true) {
                if (urlToParseQueue.isEmpty()) {
                    break;
                }
            }
            for (Thread t : parserThreads) {
                t.interrupt();
            }
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

        consumerThreadSize = Integer.parseInt(appConfiguration.getProperty("consumer.threads.size"));
        int parserThreadSize = Integer.parseInt(appConfiguration.getProperty("parser.threads.size"));

        parserThreads = new Thread[parserThreadSize];
        for (int i = 0; i < parserThreadSize; i++) {
            parserThreads[i] = new PageParserThread(urlToParseQueue, kafkaConfiguration, cacheService);
            parserThreads[i].start();
        }

        consumerThreads = new Thread[consumerThreadSize];
        for (int i = 0; i < consumerThreadSize; i++) {
            consumerThreads[i] = new LinkConsumer(kafkaConfiguration, cacheService);
            consumerThreads[i].start();
        }
    }
}
