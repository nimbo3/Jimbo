package ir.jimbo.pageprocessor;

import ir.jimbo.commons.model.Page;
import ir.jimbo.pageprocessor.manager.HTableManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class PageProcessor extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageProcessor.class);
    private final HTableManager hTableManager;
    private final String hQualifier;

    public PageProcessor(String hTableName, String hColumnFamily, String hQualifier) throws IOException {
        hTableManager = new HTableManager(hTableName, hColumnFamily);
        this.hQualifier = hQualifier;
    }

    @Override
    public void run() {
        while (!interrupted()) {
            //TODO Read from Kafka
            Page page = new Page();
            page.getLinks().forEach((k, v) -> {
                try {
                    hTableManager.put(k, hQualifier, v);
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            });
            //TODO Write to ES
            //TODO Write to Kafka
        }
    }
}
