package ir.jimbo.espageprocessor;

import com.codahale.metrics.Histogram;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.commons.model.Page;
import ir.jimbo.espageprocessor.config.ApplicationConfiguration;
import ir.jimbo.espageprocessor.config.ElasticSearchConfiguration;
import ir.jimbo.espageprocessor.config.HConfig;
import ir.jimbo.espageprocessor.config.JConfig;
import ir.jimbo.espageprocessor.manager.ElasticSearchService;
import ir.jimbo.espageprocessor.thread.PageFetcherThread;
import ir.jimbo.espageprocessor.thread.PageIndexerThread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static final List<Thread> fetcherThreads = new ArrayList<>();
    private static final List<Thread> adderThreads = new ArrayList<>();
    private static ArrayBlockingQueue<Page> queue;

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        MetricConfiguration metrics = MetricConfiguration.getInstance();
        final JConfig jConfig = JConfig.getInstance();
        ElasticSearchConfiguration elasticSearchConfiguration = ElasticSearchConfiguration.getInstance();
        ElasticSearchService elasticSearchService = new ElasticSearchService(elasticSearchConfiguration);
        int numberOfRetry = elasticSearchConfiguration.getNumberOfRetry();

        int threadCount = Integer.parseInt(jConfig.getPropertyValue("processor.threads.num"));
        HConfig hConfig = HConfig.getInstance();

        String hTableName = hConfig.getPropertyValue("tableName");
        String hColumnFamily = hConfig.getPropertyValue("columnFamily");
        ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration();
        int queueSize = applicationConfiguration.getQueueSize();
        queue = new ArrayBlockingQueue<>(queueSize);

        int adderThreadCount = applicationConfiguration.getAdderThreadCount();
        int fetcherThreadCount = applicationConfiguration.getFetcherThreadCount();

        for (int i = 0; i < adderThreadCount; i++) {
            PageIndexerThread pageIndexerThread = new PageIndexerThread(applicationConfiguration, queue, elasticSearchService, metrics, numberOfRetry,
                    hTableName, hColumnFamily);
            pageIndexerThread.start();
            adderThreads.add(pageIndexerThread);

        }
        for (int i = 0; i < fetcherThreadCount; i++) {
            PageFetcherThread pageFetcherThread = new PageFetcherThread(applicationConfiguration, queue, metrics);
            pageFetcherThread.start();
            fetcherThreads.add(pageFetcherThread);
        }

        aliveThreadCounter(metrics);
        queueSizeChecker(metrics);
    }


    private static void queueSizeChecker(MetricConfiguration metrics) {
        final long duration = Long.parseLong("100");
        Histogram histogram = metrics.getNewHistogram(metrics.getProperty("es-pp.queue.size"));
        histogram.update(queue.size());
        Thread thread = new Thread(() -> {
            while (true) {
                histogram.update(queue.size());
                try {
                    Thread.sleep(duration);
                } catch (Exception e) {
                    LOGGER.error("checker thread for counting queue size died", e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }


    private static void aliveThreadCounter(MetricConfiguration metrics) {
        final long duration = Long.parseLong("100");
        Thread thread = new Thread(() -> {
            Histogram adder = metrics.getNewHistogram("espp.adder.thread.histogram");
            Histogram fetcher = metrics.getNewHistogram("espp.fetcher.thread.histogram");
            while (true) {
                try {
                    adder.update(getCountWakeThreads(adderThreads));
                    fetcher.update(getCountWakeThreads(fetcherThreads));
                    Thread.sleep(duration);
                } catch (Exception e) {
                    LOGGER.error("checker thread for counting alive threads died", e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private static int getCountWakeThreads(List<Thread> threads) {
        int count = 0;
        for (Thread parser : threads) {
            if (parser.isAlive()) {
                count++;
            }
        }
        return count;
    }
}