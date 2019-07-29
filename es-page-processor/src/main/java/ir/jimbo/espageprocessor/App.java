package ir.jimbo.espageprocessor;

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
        final JConfig jConfig = JConfig.getInstance();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                HTableManager.closeConnection();
            pageProcessors.forEach(Thread::interrupt);
//            } catch (IOException e) {
//                LOGGER.error("", e);
//            }
        }));
        ElasticSearchConfiguration elasticSearchConfiguration = ElasticSearchConfiguration.getInstance();
        ElasticSearchService elasticSearchService = new ElasticSearchService(elasticSearchConfiguration);

        int threadCount = Integer.parseInt(jConfig.getPropertyValue("processor.threads.num"));

        for (int i = 0; i < threadCount; i++) {
            final PageProcessorThread pageProcessorThread = new PageProcessorThread(elasticSearchService);
            pageProcessors.add(pageProcessorThread);
            pageProcessorThread.start();
        }
    }
}