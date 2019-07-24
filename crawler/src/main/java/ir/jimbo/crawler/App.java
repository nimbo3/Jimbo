package ir.jimbo.crawler;


import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.service.CacheService;
import ir.jimbo.crawler.thread.PageParserThread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static Thread[] parserThreads;
    static LinkedBlockingQueue<String> linkQueue;
    private static Thread[] consumerThreads;
    private static RedisConfiguration redisConfiguration;
    private static KafkaConfiguration kafkaConfiguration;
    private static AppConfiguration appConfiguration;

    public static void main(String[] args) {
        LOGGER.info("crawler app starting...");
        addShutDownHook();
        initializeConfigurations();
        CacheService cacheService = new CacheService(redisConfiguration);

        linkQueue = new LinkedBlockingQueue<>();

        int consumerThreadSize = appConfiguration.getLinkConsumerSize();
        int parserThreadSize = appConfiguration.getPageParserSize();
        parserThreads = new Thread[parserThreadSize];
        for (int i = 0; i < parserThreadSize; i++) {
            parserThreads[i] = new PageParserThread(linkQueue, kafkaConfiguration, cacheService);
            parserThreads[i].start();
        }
        consumerThreads = new Thread[consumerThreadSize];
        for (int i = 0; i < consumerThreadSize; i++) {
            consumerThreads[i] = new LinkConsumer(kafkaConfiguration, cacheService);
            consumerThreads[i].start();
        }
    }

    private static void addShutDownHook() {
        LOGGER.info("starting shutdown hook...");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("start interrupting consumer threads");
            for (Thread t : consumerThreads) {
                t.interrupt();
            }
            LOGGER.info("end interrupting consumer threads");
            while (true) {
                LOGGER.info("waiting for queue to become empty...");
                if (linkQueue.isEmpty()) {
                    break;
                } else {
                    try {
                        Thread.sleep(500);
                    } catch (Exception e) {
                        LOGGER.error("error in thread.sleep", e);
                    }
                }
            }
            LOGGER.info("queue is empty now\nstart interrupting parser threads...");
            for (Thread t : parserThreads) {
                t.interrupt();
            }
            LOGGER.info("parser threads interrupted");
        }));
    }

    private static void initializeConfigurations() {
        redisConfiguration = null;
        try {
            redisConfiguration = new RedisConfiguration();
        } catch (IOException e) {
            LOGGER.error("error loading redis configs", e);
            System.exit(-1);
        }

        kafkaConfiguration = null;
        try {
            kafkaConfiguration = new KafkaConfiguration();
        } catch (IOException e) {
            LOGGER.error("error loading kafka configs", e);
            System.exit(-1);
        }

        appConfiguration = null;
        try {
            appConfiguration = new AppConfiguration();
        } catch (IOException e) {
            LOGGER.error("error loading app configs", e);
            System.exit(-1);
        }
    }
}
