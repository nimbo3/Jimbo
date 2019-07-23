package ir.jimbo.crawler;

import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.kafka.PageProducer;
import ir.jimbo.crawler.parse.AddPageToKafka;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Parsing {

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

    void init(PageProducer producer, String urlTopicName, String pagesTopicName) {
        for (int i = 0; i < threadsNumber; i++) {
            threadPool.submit(new AddPageToKafka(producer, urlTopicName, pagesTopicName));
        }
    }

}
