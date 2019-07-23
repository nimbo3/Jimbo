package ir.jimbo.crawler.config;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class AppConfiguration {

    private Properties properties = new Properties();
    private int pageParserSize;
    private int linkConsumerSize;

    public AppConfiguration() throws IOException {
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("appConfig.properties")));

        linkConsumerSize = Integer.parseInt(getProperty("consumer.threads.size"));
        pageParserSize = Integer.parseInt(getProperty("parser.threads.size"));

    }

    public int getPageParserSize() {
        return pageParserSize;
    }

    public int getLinkConsumerSize() {
        return linkConsumerSize;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

}
