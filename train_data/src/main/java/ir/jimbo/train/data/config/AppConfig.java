package ir.jimbo.train.data.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class AppConfig {
    private static final Logger LOGGER = LogManager.getLogger(AppConfig.class);

    private int threadPoolSize;
    private String urlFilePath;
    private int localQueueSize;
    private List<String> startingURLs;

    public AppConfig() throws IOException, URISyntaxException {
        Properties properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                "config.properties")));
        initValues(properties);
    }

    public AppConfig(String path) throws IOException, URISyntaxException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(path));
        initValues(properties);
    }

    private void initValues(Properties properties) throws URISyntaxException {
        threadPoolSize = Integer.parseInt(properties.getProperty("app.thread.pool.size"));
        urlFilePath = properties.getProperty("app.urls.file.path");
        localQueueSize = Integer.parseInt(properties.getProperty("app.local.queue.size"));
        startingURLs = Arrays.stream(properties.getProperty("app.starting.url").split(",")).map(url -> {
            try {
                return new URI(properties.getProperty("app.starting.url")).normalize().toString();
            } catch (URISyntaxException e) {
                LOGGER.error("", e);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
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

    public List<String> getStartingURLs() {
        return startingURLs;
    }
}
