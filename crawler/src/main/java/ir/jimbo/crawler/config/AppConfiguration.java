package ir.jimbo.crawler.config;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class AppConfiguration {

    private Properties properties = new Properties();

    public AppConfiguration() throws IOException {
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("appConfig.properties")));
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

}
