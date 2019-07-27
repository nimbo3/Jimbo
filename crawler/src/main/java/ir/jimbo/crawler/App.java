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
    private static LinkConsumer[] consumers;
    private static PageParserThread[] producers;

    public static void main(String[] args) {
        LOGGER.info("crawler app starting...");
        initializeConfigurations(args);
        CacheService cacheService = new CacheService(redisConfiguration);

        linkQueue = new ArrayBlockingQueue<>(appConfiguration.getQueueSize());

        int consumerThreadSize = appConfiguration.getLinkConsumerSize();
        int parserThreadSize = appConfiguration.getPageParserSize();
        cacheService.addUrl("al");
        System.out.println(cacheService.isUrlExists("al"));

        CountDownLatch parserLatch = new CountDownLatch(parserThreadSize);
        CountDownLatch consumerLatch = new CountDownLatch(consumerThreadSize);

        addShutDownHook(parserLatch, parserThreadSize, consumerLatch, consumerThreadSize);

        LOGGER.info("starting parser threads");
        producers = new PageParserThread[parserThreadSize];
        for (int i = 0; i < parserThreadSize; i++) {
            producers[i] = new PageParserThread(linkQueue, kafkaConfiguration, parserLatch);
            producers[i].start();
        }

        consumers = new LinkConsumer[consumerThreadSize];
        LOGGER.info("starting consumer threads");
        for (int i = 0; i < consumerThreadSize; i++) {
            consumers[i] = new LinkConsumer(kafkaConfiguration, cacheService, consumerLatch);
            consumers[i].start();
        }
        LOGGER.info("end starting threads");
    }

    private static void addShutDownHook(CountDownLatch parserLatch, int parserThreadSize, CountDownLatch consumerLatch, int consumerThreadSize) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("starting shutdown hook...");
            consumerThreadInterruption(consumerLatch, consumerThreadSize);
            queueEmptyChecker();
            parserThreadInterruption(parserLatch, parserThreadSize);
        }));
    }

    private static void parserThreadInterruption(CountDownLatch parserThreadSize, int producersThreadSize) {

        LOGGER.info("start interrupting parser threads...");
        for (int i = 0; i < producersThreadSize; i++) {
            producers[i].close();
        }
        try {
            LOGGER.info("before parser threads");
            parserThreadSize.await();
            LOGGER.info("after parser threads");
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

    private static void consumerThreadInterruption(CountDownLatch countDownLatch, int consumerThreadSize) {
        LOGGER.info("start interrupting consumer threads");
        for (int i = 0; i < consumerThreadSize; i++) {
            consumers[i].close();
        }
        try {
            LOGGER.info("before consumer await. size : " + countDownLatch.getCount());
            countDownLatch.await();
            LOGGER.info("after consumer await");
        } catch (InterruptedException e) {
            LOGGER.warn("interrupted exception in closing consumer threads");
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
