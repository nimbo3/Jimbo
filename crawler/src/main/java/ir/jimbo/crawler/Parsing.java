package ir.jimbo.crawler;

import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.parse.AddPageToKafka;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Parsing {

    private Logger logger = LogManager.getLogger(this.getClass());
    protected ArrayBlockingQueue<String> urlToParseQueue;
    private ExecutorService threadPool;
    protected RedisConnection redis;
    protected KafkaConfiguration kafkaConfiguration;

    protected Parsing() {

    }

    Parsing(AppConfiguration data, RedisConnection redis, KafkaConfiguration kafkaConfiguration) {
        int threadsNumber = Integer.parseInt(data.getProperty("thread.pool.core.size"));
        urlToParseQueue = new ArrayBlockingQueue<>(Integer.parseInt(data.getProperty("array.blocking.queue.init.size")));
        threadPool = Executors.newFixedThreadPool(threadsNumber);
        this.redis = redis;
        this.kafkaConfiguration = kafkaConfiguration;
        for (int i = 0; i < threadsNumber; i++) {
            threadPool.submit(new AddPageToKafka());
            logger.info("a new thread started");
        }
    }
}
