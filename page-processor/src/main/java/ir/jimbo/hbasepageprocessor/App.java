package ir.jimbo.hbasepageprocessor;

import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.hbasepageprocessor.config.HConfig;
import ir.jimbo.hbasepageprocessor.config.JConfig;
import ir.jimbo.hbasepageprocessor.manager.HTableManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static final List<PageProcessorThread> pageProcessors = new ArrayList<>();

    public static void main(String[] args) throws IOException {

        MetricConfiguration metrics = new MetricConfiguration();

        final JConfig jConfig = JConfig.getInstance();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                pageProcessors.forEach(Thread::interrupt);
                pageProcessors.forEach(o -> {
                    try {
                        o.close();
                    } catch (IOException e) {
                        LOGGER.error("", e);
                    }
                });
                HTableManager.closeConnection();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }));
        HConfig hConfig = HConfig.getInstance();

        String hTableName = hConfig.getPropertyValue("tableName");
        String hColumnFamily = hConfig.getPropertyValue("columnFamily");
        int threadCount = Integer.parseInt(jConfig.getPropertyValue("processor.threads.num"));
        LOGGER.info("Number of threads to run: " + threadCount);
        for (int i = 0; i < threadCount; i++) {
            final PageProcessorThread pageProcessorThread = new PageProcessorThread(hTableName, hColumnFamily, metrics);
            pageProcessors.add(pageProcessorThread);
            pageProcessorThread.start();
        }
    }
}