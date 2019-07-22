package ir.jimbo.pageprocessor;

import ir.jimbo.pageprocessor.config.Config;
import ir.jimbo.pageprocessor.config.HConfig;
import ir.jimbo.pageprocessor.manager.HTableManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);

    private static String hTableName;
    private static String hColumnFamily;
    private static String hQualifier;

    static {
        Config hConfig = HConfig.getInstance();
        hTableName = hConfig.getPropertyValue("tableName");
        hColumnFamily = hConfig.getPropertyValue("columnFamily");
        hQualifier = hConfig.getPropertyValue("qualifier");
    }

    public static void main(String[] args) throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                HTableManager.closeConnection();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }));
        for (int i = 0; i < 10; i++) {
            new PageProcessor(hTableName, hColumnFamily, hQualifier).start();
        }
    }
}
