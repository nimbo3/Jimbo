package ir.jimbo.hbasepageprocessor;

import ir.jimbo.hbasepageprocessor.config.HConfig;
import ir.jimbo.hbasepageprocessor.config.JConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static final List<Thread> pageProcessors = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        final JConfig jConfig = JConfig.getInstance();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                HTableManager.closeConnection();
            pageProcessors.forEach(Thread::interrupt);
//            } catch (IOException e) {
//                LOGGER.error("", e);
//            }
        }));
        HConfig hConfig = HConfig.getInstance();

        String hTableName = hConfig.getPropertyValue("tableName");
        String hColumnFamily = hConfig.getPropertyValue("columnFamily");
        int threadCount = Integer.parseInt(jConfig.getPropertyValue("processor.threads.num"));
        for (int i = 0; i < threadCount; i++) {
            final PageProcessorThread pageProcessorThread = new PageProcessorThread(hTableName, hColumnFamily);
            pageProcessors.add(pageProcessorThread);
            pageProcessorThread.start();
        }
    }
}