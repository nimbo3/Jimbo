package ir.jimbo.train.data.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class AppConfig {

    private int threadPoolSize;
    private String urlFilePath;
    private int localQueueSize;

    public AppConfig() throws IOException {
        Properties properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                "config.properties")));
        initValues(properties);
    }

    public AppConfig(String path) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(path));
        initValues(properties);
    }

    private void initValues(Properties properties) {
        threadPoolSize = Integer.parseInt(properties.getProperty("app.thread.pool.size"));
        urlFilePath = properties.getProperty("app.urls.file.path");
        localQueueSize = Integer.parseInt(properties.getProperty("app.local.queue.size"));
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public String getUrlFilePath() {
        return urlFilePath;
    }

    public int getLocalQueueSize() {
        return localQueueSize;
    }
}
