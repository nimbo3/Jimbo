package ir.jimbo.espagemigrator;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.espagemigrator.manager.ElasticSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class PageMigratorThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageMigratorThread.class);
    private ElasticSearchService esService;
    private MetricConfiguration metrics;
    private int numberOfRetry;

    public PageMigratorThread(ElasticSearchService esService, MetricConfiguration metrics, int numberOfRetry) {
        this.esService = esService;
        this.setName(this.getName() + "es-page processor thread");
        this.metrics = metrics;
        this.numberOfRetry = numberOfRetry;
    }

    @Override
    public void run() {
        Histogram histogram = metrics.getNewHistogram(metrics.getProperty("elastic.pages.histogram.name"));
        Timer processTime = metrics.getNewTimer(metrics.getProperty("elastic.process.timer.name"));
        histogram.update(0);
        while (!interrupted()) {
            try {
                Timer.Context timerContext = processTime.time();
                List<ElasticPage> pages = esService.getSourcePages();
                histogram.update(pages.size());
                boolean isAdded = false;
                int retryCounter = 0;
                long start = System.currentTimeMillis();
                while (!isAdded && retryCounter < numberOfRetry) {
                    isAdded = esService.insertPages(pages);
                    if (!isAdded) {
                        retryCounter++;
                        Thread.sleep(100);
                        LOGGER.info("ES insertion failed.");
                    }
                }
                long end = System.currentTimeMillis();
                if (!isAdded) {
                    throw new Exception("pages don't insert " + Arrays.toString(pages.toArray()));
                }

                timerContext.stop();
                final String message = String.format("record_size: %d %dre%d", pages.size(), start - end, retryCounter);
                LOGGER.info(message);
            } catch (Exception e) {
                LOGGER.error("error in process messages", e);
            }
        }
    }
}
