package ir.jimbo.crawler.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class AppConfiguration {

    private int pageParserSize;
    private int linkConsumerSize;

    public AppConfiguration() throws IOException {
        Properties properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("appConfig.properties")));

        linkConsumerSize = Integer.parseInt(properties.getProperty("consumer.threads.size"));
        pageParserSize = Integer.parseInt(properties.getProperty("parser.threads.size"));

    }

    public AppConfiguration(String path) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(path));
        linkConsumerSize = Integer.parseInt(properties.getProperty("consumer.threads.size"));
        pageParserSize = Integer.parseInt(properties.getProperty("parser.threads.size"));

    }

    public int getPageParserSize() {
        return pageParserSize;
    }

    public int getLinkConsumerSize() {
        return linkConsumerSize;
    }

}
