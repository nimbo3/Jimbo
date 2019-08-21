package ir.jimbo.espageprocessor.thread;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.commons.model.Page;
import ir.jimbo.espageprocessor.assets.HRow;
import ir.jimbo.espageprocessor.config.ApplicationConfiguration;
import ir.jimbo.espageprocessor.manager.ElasticSearchService;
import ir.jimbo.espageprocessor.manager.HTableManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PageIndexerThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageFetcherThread.class);
    private final HTableManager hTableManager;
    private final int timeout;
    private ElasticSearchService esService;
    private MetricConfiguration metrics;
    private int numberOfRetry;
    private ArrayBlockingQueue<Page> queue;
    private int bulkSize;

    public PageIndexerThread(ApplicationConfiguration appConfig, ArrayBlockingQueue<Page> blockingQueue, ElasticSearchService esService
            , MetricConfiguration metrics, int numberOfRetry, String hTableName
            , String hColumnFamily) throws IOException, NoSuchAlgorithmException {
        bulkSize = esService.getConfiguration().getBulkSize();
        hTableManager = new HTableManager(hTableName, hColumnFamily, "HBaseHealthChecker", metrics);
        this.esService = esService;
        this.setName(this.getName() + "es-page processor thread");
        this.metrics = metrics;
        this.numberOfRetry = numberOfRetry;
        this.queue = blockingQueue;
        this.bulkSize = appConfig.getBulkSize();
        this.timeout = appConfig.getQueueTimeOut();
    }

    @Override
    public void run() {
        Timer processTime = metrics.getNewTimer(metrics.getProperty("elastic.process.timer.name"));
        Counter insertedPagesCounter = metrics.getNewCounter("insertedPagesCounter");
        Timer insertEsTime = metrics.getNewTimer("insertEsTime");
        Timer putFlagTime = metrics.getNewTimer("hbasePutFlagTime");

        while (!interrupted()) {
            try {
                List<Page> pages = new ArrayList<>();
                List<HRow> flags = new ArrayList<>();
                while (pages.size() < bulkSize) {
                    Page take = queue.poll(timeout, TimeUnit.MILLISECONDS);
                    if (take == null)
                        break;
                    pages.add(take);
                    flags.add(new HRow(take.getUrl(), "f", 1));
                }
                Timer.Context timerContext = processTime.time();

                boolean isAdded = false;
                int retryCounter = 0;
                long start = System.currentTimeMillis();
                Timer.Context esTime = insertEsTime.time();
                while (!isAdded && retryCounter < numberOfRetry) {
                    isAdded = esService.insertPages(pages);
                    if (!isAdded) {
                        retryCounter++;
                        Thread.sleep(100);
                        LOGGER.info("ES insertion failed.");
                    }
                }

                esTime.stop();
                long end = System.currentTimeMillis();
                if (!isAdded) {
                    throw new Exception("pages don't insert " + Arrays.toString(pages.toArray()));
                }

                Timer.Context hbaseTime = putFlagTime.time();
                hTableManager.put(flags);
                hbaseTime.stop();
                insertedPagesCounter.inc(pages.size());
                timerContext.stop();
                LOGGER.info("record_size: " + pages.size() + " " + (start - end) + "re" + retryCounter);
            } catch (Exception e) {
                LOGGER.error("error in process messages", e);
            }
        }
    }
}
