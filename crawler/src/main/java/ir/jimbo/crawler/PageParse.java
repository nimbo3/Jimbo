package ir.jimbo.crawler;

import ir.jimbo.crawler.parse.PageParser;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PageParse {

    protected ArrayBlockingQueue<String> urlToParseQueue = new ArrayBlockingQueue<>(100); // 100 Must go to configuration
    ExecutorService threadPool = Executors.newFixedThreadPool(100);

    public void start() {
        for (int i = 0; i < 100; i++) { // 100 must change
            threadPool.submit(new PageParser());
        }
    }

}
