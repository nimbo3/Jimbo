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

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        final JConfig jConfig = JConfig.getInstance();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                HTableManager.closeConnection();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }));
        Config hConfig = HConfig.getInstance();
        ElasticSearchConfiguration elasticSearchConfiguration = new ElasticSearchConfiguration();
        ElasticSearchService elasticSearchService = new ElasticSearchService(elasticSearchConfiguration);

        String hTableName = hConfig.getPropertyValue("tableName");
        String hColumnFamily = hConfig.getPropertyValue("columnFamily");
        String hQualifier = hConfig.getPropertyValue("qualifier");

        for (int i = 0; i < Integer.parseInt(jConfig.getPropertyValue("processor.threads.num")); i++) {
            new PageProcessorThread(hTableName, hColumnFamily, hQualifier, elasticSearchService).start();
        }
    }
}
