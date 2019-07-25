package ir.jimbo.crawler;


import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import ir.jimbo.crawler.service.CacheService;
import ir.jimbo.crawler.thread.PageParserThread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);
    static ArrayBlockingQueue<String> linkQueue;
    private static RedisConfiguration redisConfiguration;
    private static KafkaConfiguration kafkaConfiguration;
    private static AppConfiguration appConfiguration;

    public static void main(String[] args) throws InterruptedException {
        LOGGER.info("crawler app starting...");
        initializeConfigurations(args);
        CacheService cacheService = new CacheService(redisConfiguration);

        linkQueue = new ArrayBlockingQueue<>(appConfiguration.getQueueSize());

        int consumerThreadSize = appConfiguration.getLinkConsumerSize();
        int parserThreadSize = appConfiguration.getPageParserSize();

        CountDownLatch parserLatch = new CountDownLatch(parserThreadSize);
        CountDownLatch consumerLatch = new CountDownLatch(consumerThreadSize);

        addShutDownHook(parserLatch, consumerLatch);

        Thread.sleep(1000);

        new LinkConsumer(kafkaConfiguration, cacheService, consumerLatch).start();
        new PageParserThread(linkQueue, kafkaConfiguration, parserLatch).start();

    }

    private static void addShutDownHook(CountDownLatch parserLatch, CountDownLatch consumerLatch) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("starting shutdown hook...");
            consumerThreadInterruption(consumerLatch);
            queueEmptyChecker();
            parserThreadInterruption(parserLatch);
        }));
    }

    private static void parserThreadInterruption(CountDownLatch parserThreadSize) {

        LOGGER.info("start interrupting parser threads...");
        PageParserThread.repeat = false;
        try {
            Thread.sleep(1000);
            parserThreadSize.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info("parser threads interrupted");
    }

    private static void queueEmptyChecker() {
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
        LOGGER.info("queue is empty now");
    }

    private static void consumerThreadInterruption(CountDownLatch consumerThreadSize) {
        LOGGER.info("start interrupting consumer threads");
        LinkConsumer.repeat = false;
        try {
            Thread.sleep(1000);
            consumerThreadSize.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info("end interrupting consumer threads");
    }

    private static void initializeConfigurations(String[] args) {

        String redisPath = null;
        String kafkaPath = null;
        String appPath = null;

        for (String path : args) {
            String key = path.split(":")[0];
            String value = path.split(":")[1];
            switch (key) {
                case "redis":
                    redisPath = value;
                    break;
                case "kafka":
                    kafkaPath = value;
                    break;
                case "app":
                    appPath = value;
                    break;
                default:
                    //
            }
        }

        redisConfiguration = null;
        try {
            if (redisPath == null) {
                redisConfiguration = new RedisConfiguration();
            } else {
                redisConfiguration = new RedisConfiguration(redisPath);
            }
        } catch (IOException e) {
            LOGGER.error("error loading redis configs", e);
            System.exit(-1);
        }

        kafkaConfiguration = null;
        try {
            if (kafkaPath == null) {
                kafkaConfiguration = new KafkaConfiguration();
            } else {
                kafkaConfiguration = new KafkaConfiguration(kafkaPath);
            }
        } catch (IOException e) {
            LOGGER.error("error loading kafka configs", e);
            System.exit(-1);
        }

        appConfiguration = null;
        try {
            if (appPath == null) {
                appConfiguration = new AppConfiguration();
            } else {
                appConfiguration = new AppConfiguration(appPath);
            }
        } catch (IOException e) {
            LOGGER.error("error loading app configs", e);
            System.exit(-1);
        }
    }
}
