package ir.jimbo.crawler;

import ir.jimbo.crawler.config.AppConfiguration;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.crawler.kafka.MyProducer;
import ir.jimbo.crawler.parse.PageParseAndAddToKafka;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;

public class PageParse {

    protected ArrayBlockingQueue<String> urlToParseQueue;
    ExecutorService threadPool;
    int threadsNumber;
    protected RedisConnection redis;

    protected PageParse() {

    }

    PageParse(AppConfiguration data, RedisConnection redis) {
        threadsNumber = Integer.parseInt(data.getProperty("thread.pool.core.size"));
        urlToParseQueue = new ArrayBlockingQueue<>(Integer.parseInt(data.getProperty("array.blocking.queue.init.size")));
        threadPool = Executors.newFixedThreadPool(threadsNumber);
        this.redis = redis;
    }

    public void start(MyProducer producer, String urlTopicName) {
        for (int i = 0; i < threadsNumber; i++) {
            threadPool.submit(new PageParseAndAddToKafka(producer, urlTopicName));
        }
    }

}
