package ir.jimbo.pageprocessor;

import ir.jimbo.pageprocessor.config.Config;
import ir.jimbo.pageprocessor.config.ElasticSearchConfiguration;
import ir.jimbo.pageprocessor.config.HConfig;
import ir.jimbo.pageprocessor.config.JConfig;
import ir.jimbo.pageprocessor.manager.ElasticSearchService;
import ir.jimbo.pageprocessor.manager.HTableManager;
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
            try {
                HTableManager.closeConnection();
                pageProcessors.forEach(Thread::interrupt);
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }));
        Config hConfig = HConfig.getInstance();
        ElasticSearchConfiguration elasticSearchConfiguration = ElasticSearchConfiguration.getInstance();
        ElasticSearchService elasticSearchService = new ElasticSearchService(elasticSearchConfiguration);

        String hTableName = hConfig.getPropertyValue("tableName");
        String hColumnFamily = hConfig.getPropertyValue("columnFamily");
        String hQualifier = hConfig.getPropertyValue("qualifier");
        for (int i = 0; i < Integer.parseInt(jConfig.getPropertyValue("processor.threads.num")); i++) {
            final PageProcessorThread pageProcessorThread = new PageProcessorThread(hTableName, hColumnFamily, hQualifier, elasticSearchService);
            pageProcessors.add(pageProcessorThread);
            pageProcessorThread.start();
        }
    }
}
