package ir.jimbo.crawler;

import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.kafka.PageAndLinkProducer;
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
    private int threadsNumber;
    protected RedisConnection redis;

    protected Parsing() {

    }

    Parsing(AppConfiguration data, RedisConnection redis) {
        threadsNumber = Integer.parseInt(data.getProperty("thread.pool.core.size"));
        urlToParseQueue = new ArrayBlockingQueue<>(Integer.parseInt(data.getProperty("array.blocking.queue.init.size")));
        threadPool = Executors.newFixedThreadPool(threadsNumber);
        this.redis = redis;
    }

    void init(PageAndLinkProducer producer, String urlTopicName, String pagesTopicName) {
        for (int i = 0; i < threadsNumber; i++) {
            threadPool.submit(new AddPageToKafka(producer, urlTopicName, pagesTopicName));
            System.out.println("a thread added");
        }
    }

}
