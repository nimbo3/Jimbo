package ir.jimbo.crawler;

import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.service.CacheService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;

public class Parser {
    private Logger logger = LogManager.getLogger(this.getClass());
    protected ArrayBlockingQueue<String> urlToParseQueue;
    private ExecutorService threadPool;
    protected CacheService redis;
    protected KafkaConfiguration kafkaConfiguration;

    protected Parser() {

    Parsing(AppConfiguration appConfiguration, RedisConnection redis, KafkaConfiguration kafkaConfiguration) {
        int threadsNumber = Integer.parseInt(appConfiguration.getProperty("thread.pool.core.size"));
    }

    Parser(AppConfiguration data, CacheService redis, KafkaConfiguration kafkaConfiguration) {
        int threadsNumber = Integer.parseInt(data.getProperty("thread.pool.core.size"));
        urlToParseQueue = new ArrayBlockingQueue<>(Integer.parseInt(data.getProperty("array.blocking.queue.init.size")));
        threadPool = Executors.newFixedThreadPool(threadsNumber);
        this.redis = redis;
        this.kafkaConfiguration = kafkaConfiguration;
    }
}
