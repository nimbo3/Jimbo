package ir.jimbo.espagemigrator;

import com.codahale.metrics.Histogram;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.espagemigrator.config.ElasticSearchConfiguration;
import ir.jimbo.espagemigrator.config.JConfig;
import ir.jimbo.espagemigrator.manager.ElasticSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static final List<Thread> pageMigrators = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        MetricConfiguration metrics = MetricConfiguration.getInstance();
        final JConfig jConfig = JConfig.getInstance();
        ElasticSearchConfiguration elasticSearchConfiguration = ElasticSearchConfiguration.getInstance();
        ElasticSearchService elasticSearchService = new ElasticSearchService(elasticSearchConfiguration);
        int numberOfRetry = elasticSearchConfiguration.getNumberOfRetry();

        int threadCount = Integer.parseInt(jConfig.getPropertyValue("processor.threads.num"));


        for (int i = 0; i < threadCount; i++) {
            final PageMigratorThread pageMigratorThread = new PageMigratorThread(elasticSearchService, metrics
                    , numberOfRetry);
            pageMigrators.add(pageMigratorThread);
            pageMigratorThread.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pageMigrators.forEach(Thread::interrupt);
            while (pageMigrators.stream().anyMatch(Thread::isAlive)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    //ignore
                    break;
                }
            }
            elasticSearchService.getClient().close();
        }));

        aliveThreadCounter(metrics, Long.parseLong(metrics.getProperty("metric.check.threads.duration.millis")),
                metrics.getProperty("checker.thread.name"));
    }

    private static void aliveThreadCounter(MetricConfiguration metrics, long duration, String counterName) {
        new Thread(() -> {
            Histogram hBaseThreadNum = metrics.getNewHistogram(counterName);
            while (true) {
                hBaseThreadNum.update(getAllWakeThreads());
                try {
                    Thread.sleep(duration);
                } catch (Exception e) {
                    LOGGER.error("checker thread for counting alive threads died", e);
                }
            }
        }).start();
    }

    private static int getAllWakeThreads() {
        int count = 0;
        for (Thread parser : pageMigrators) {
            if (parser.isAlive()) {
                count++;
            }
        }
        return count;
    }
}