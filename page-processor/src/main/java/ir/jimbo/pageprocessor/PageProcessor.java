package ir.jimbo.pageprocessor;

import ir.jimbo.pageprocessor.manager.HTableManager;

import java.io.IOException;

public class PageProcessor extends Thread {
    private final HTableManager hTableManager;
    private final String hQualifier;

    public PageProcessor(String hTableName, String hColumnFamily, String hQualifier) throws IOException {
        hTableManager = new HTableManager(hTableName, hColumnFamily);
        this.hQualifier = hQualifier;
    }

    @Override
    public void run() {
        //TODO Read from Kafka
        //TODO Write to HBase
        //TODO Write to ES
        //TODO Write to Kafka
    }
}
