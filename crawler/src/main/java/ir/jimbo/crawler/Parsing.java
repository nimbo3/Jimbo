package ir.jimbo.crawler;

import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.parse.AddPageToKafka;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;

public class Parsing {

    private static Logger logger = LogManager.getLogger(this.getClass());
    protected static RedisConnection redis;
    protected static KafkaConfiguration kafkaConfiguration;

    Parsing(AppConfiguration appConfiguration, RedisConnection redis, KafkaConfiguration kafkaConfiguration) {
        int threadsNumber = Integer.parseInt(appConfiguration.getProperty("thread.pool.core.size"));
        this.redis = redis;
        this.kafkaConfiguration = kafkaConfiguration;
    }
}
