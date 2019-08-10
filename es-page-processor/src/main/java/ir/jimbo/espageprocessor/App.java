package ir.jimbo.espageprocessor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.espageprocessor.config.ElasticSearchConfiguration;
import ir.jimbo.espageprocessor.config.JConfig;
import ir.jimbo.espageprocessor.manager.ElasticSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static final List<Thread> pageProcessors = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        MetricConfiguration metrics = new MetricConfiguration();
        final JConfig jConfig = JConfig.getInstance();
        ElasticSearchConfiguration elasticSearchConfiguration = ElasticSearchConfiguration.getInstance();
        ElasticSearchService elasticSearchService = new ElasticSearchService(elasticSearchConfiguration);
        int numberOfRetry = elasticSearchConfiguration.getNumberOfRetry();

        int threadCount = Integer.parseInt(jConfig.getPropertyValue("processor.threads.num"));

        for (int i = 0; i < threadCount; i++) {
            final PageProcessorThread pageProcessorThread = new PageProcessorThread(elasticSearchService, metrics , numberOfRetry);
            pageProcessors.add(pageProcessorThread);
            pageProcessorThread.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pageProcessors.forEach(Thread::interrupt);
            elasticSearchService.getClient().close();
        }));

        aliveThreadCounter(metrics, Long.parseLong(metrics.getProperty("metric.check.threads.duration.milis")),
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
        for (Thread parser: pageProcessors) {
            if (parser.isAlive()) {
                count ++;
            }
        }
        return count;
    }
}