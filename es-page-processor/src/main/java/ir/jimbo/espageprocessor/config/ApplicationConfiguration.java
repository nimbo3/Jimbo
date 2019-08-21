package ir.jimbo.espageprocessor.config;

import lombok.Getter;

import java.io.IOException;

@Getter
public class ApplicationConfiguration  extends Config{
    private static final String PRE_FIX = "application";

    private int queueSize;
    private int queueTimeOut;
    private int bulkSize;
    private int fetcherThreadCount;
    private int adderThreadCount;

    private ApplicationConfiguration(String prefix) throws IOException {
        super(prefix);
    }

    public ApplicationConfiguration() throws IOException {
        super(PRE_FIX);
        this.queueSize = Integer.parseInt(getPropertyValue("queue.size"));
        this.fetcherThreadCount = Integer.parseInt(getPropertyValue("fetcher.thread.count"));
        this.adderThreadCount = Integer.parseInt(getPropertyValue("adder.thread.count"));
        this.queueTimeOut = Integer.parseInt(getPropertyValue("queue.timeout.millis"));
        this.bulkSize = Integer.parseInt(getPropertyValue("bulk.size"));
    }
}
