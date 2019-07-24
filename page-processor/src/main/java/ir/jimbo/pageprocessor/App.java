package ir.jimbo.pageprocessor;

import ir.jimbo.pageprocessor.config.Config;
import ir.jimbo.pageprocessor.config.HConfig;
import ir.jimbo.pageprocessor.manager.HTableManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                HTableManager.closeConnection();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }));
        Config hConfig = HConfig.getInstance();

        String hTableName = hConfig.getPropertyValue("tableName");
        String hColumnFamily = hConfig.getPropertyValue("columnFamily");
        String hQualifier = hConfig.getPropertyValue("qualifier");

        for (int i = 0; i < 1; i++) {
            new PageProcessorThread(hTableName, hColumnFamily, hQualifier).start();
        }
    }
}
