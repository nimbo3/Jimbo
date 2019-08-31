package ir.jimbo.crawler;


import com.codahale.metrics.Histogram;
import ir.jimbo.commons.config.MetricConfiguration;
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
import java.util.concurrent.atomic.AtomicBoolean;


public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static ArrayBlockingQueue<String> queue;
    private static RedisConfiguration redisConfiguration;
    private static KafkaConfiguration kafkaConfiguration;
    private static AppConfiguration appConfiguration;
    private static Thread[] consumers;
    private static Thread[] producers;
    private static AtomicBoolean repeat = new AtomicBoolean(true);
    public static boolean produceLink = true;

    public static void main(String[] args) throws IOException, InterruptedException {
        LOGGER.info("crawler app starting...");
        initializeConfigurations(args);
        MetricConfiguration metrics = MetricConfiguration.getInstance();    // Throws IOException
        CacheService cacheService = new CacheService(redisConfiguration);
        queue = new ArrayBlockingQueue<>(appConfiguration.getQueueSize());

        int consumerThreadSize = appConfiguration.getLinkConsumerSize();
        int parserThreadSize = appConfiguration.getPageParserSize();
        CountDownLatch parserLatch = new CountDownLatch(parserThreadSize);
        CountDownLatch consumerLatch = new CountDownLatch(consumerThreadSize);

        addShutDownHook(parserLatch, parserThreadSize, consumerLatch, consumerThreadSize);

        LOGGER.info("starting parser threads");
        producers = new PageParserThread[parserThreadSize];
        for (int i = 0; i < parserThreadSize; i++) {
            producers[i] = new PageParserThread(queue, kafkaConfiguration, parserLatch, cacheService, metrics);
            producers[i].start();
        }

        consumers = new LinkConsumer[consumerThreadSize];
        LOGGER.info("starting consumer threads");
        for (int i = 0; i < consumerThreadSize; i++) {
            consumers[i] = new LinkConsumer(kafkaConfiguration, cacheService, consumerLatch, queue, metrics);
        }
        Thread.sleep(1000);
        for (Thread consumer : consumers) {
            consumer.start();
        }
        LOGGER.info("end starting threads");
        aliveThreadCounter(metrics);
        queueSizeChecker(metrics);
    }

    private static void queueSizeChecker(MetricConfiguration metrics) {
        final long duration = Long.parseLong(metrics.getProperty("crawler.check.duration"));
        Histogram histogram = metrics.getNewHistogram(metrics.getProperty("crawler.queue.size.histogram.name"));
        histogram.update(queue.size());
        new Thread(() -> {
            while (repeat.get()) {
                histogram.update(queue.size());
                try {
                    Thread.sleep(duration);
                } catch (Exception e) {
                    LOGGER.error("checker thread for counting queue size died", e);
                }
            }
        }).start();
    }

    private static void aliveThreadCounter(MetricConfiguration metrics) {
        final long duration = Long.parseLong(metrics.getProperty("crawler.check.duration"));
        new Thread(() -> {
            Histogram consumerThreadNum = metrics.getNewHistogram(metrics.getProperty("crawler.consumer.thread.histogram.name"));
            Histogram parserThreadNum = metrics.getNewHistogram(metrics.getProperty("crawler.parser.thread.histogram.name"));
            while (repeat.get()) {
                consumerThreadNum.update(getAllWakeConsumers());
                parserThreadNum.update(getAllWakeProducers());
                try {
                    Thread.sleep(duration);
                } catch (Exception e) {
                    LOGGER.error("checker thread for counting alive threads died", e);
                }
            }
        }).start();
    }

    public static int getAllWakeConsumers() {
        int counter = 0;
        for (Thread consumer : consumers) {
            if (consumer.isAlive()) {
                counter ++;
            }
        }
        return counter;
    }

    static int getAllWakeProducers() {
        int counter = 0;
        for (Thread producer : producers) {
            if (producer.isAlive()) {
                counter ++;
            }
        }
        return counter;
    }

    private static void addShutDownHook(CountDownLatch parserLatch, int parserThreadSize, CountDownLatch consumerLatch, int consumerThreadSize) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("starting shutdown hook...");
            repeat.set(false);
            consumerThreadInterruption(consumerLatch, consumerThreadSize);
            queueEmptyChecker();
            parserThreadInterruption(parserLatch, parserThreadSize);
        }));
    }

    private static void parserThreadInterruption(CountDownLatch parsersCountDownLatch, int producersThreadSize) {

        LOGGER.info("start interrupting parser threads...");
        for (int i = 0; i < producersThreadSize; i++) {
            producers[i].interrupt();
        }
        try {
            LOGGER.info("before parser threads");
            parsersCountDownLatch.await();
            LOGGER.info("after parser threads");
        } catch (Exception e) {
            parserThreadInterruption(parsersCountDownLatch, producersThreadSize);
        }
        LOGGER.info("parser threads interrupted");
    }

    private static void queueEmptyChecker() {
        while (true) {
            LOGGER.info("waiting for queue to become empty...");
            if (queue.isEmpty()) {
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
            consumers[i].interrupt();
        }
        try {
            LOGGER.info("before consumer await. size : " + countDownLatch.getCount());
            countDownLatch.await();
            LOGGER.info("after consumer await");
        } catch (Exception e) {
            LOGGER.warn("interrupted exception in closing consumer threads");
            consumerThreadInterruption(countDownLatch, consumerThreadSize);
        }
        LOGGER.info("end interrupting consumer threads");
    }

    static void initializeConfigurations(String[] args) throws IOException {

        String redisPath = null;
        String kafkaPath = null;
        String appPath = null;

        for (String path : args) {
            String key = path.split(":")[0];
            String value = path.split(":")[1];
            switch (key.toLowerCase().trim()) {
                case "redis":
                    redisPath = value.trim();
                    break;
                case "kafka":
                    kafkaPath = value.trim();
                    break;
                case "app":
                    appPath = value.trim();
                    break;
                case "producer":
                    produceLink = Boolean.parseBoolean(value.trim());
                    break;
                default:
                    //
            }
        }

        redisConfiguration = null;
        if (redisPath == null) {
            redisConfiguration = new RedisConfiguration();
        } else {
            redisConfiguration = new RedisConfiguration(redisPath);
        }

        kafkaConfiguration = null;
        if (kafkaPath == null) {
            kafkaConfiguration = new KafkaConfiguration();
        } else {
            kafkaConfiguration = new KafkaConfiguration(kafkaPath);
        }

        appConfiguration = null;
        if (appPath == null) {
            appConfiguration = new AppConfiguration();
        } else {
            appConfiguration = new AppConfiguration(appPath);
        }
    }

    public static RedisConfiguration getRedisConfiguration() {
        return redisConfiguration;
    }

    public static AppConfiguration getAppConfiguration() {
        return appConfiguration;
    }

    public static KafkaConfiguration getKafkaConfiguration() {
        return kafkaConfiguration;
    }

    public static Thread[] getConsumers() {
        return consumers;
    }

    public static void setConsumers(Thread[] array) {
        consumers = array;
    }

    public static Thread[] getProducers() {
        return producers;
    }

    public static void setProducers(Thread[] array) {
        producers = array;
    }
}
