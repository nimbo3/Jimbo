package ir.jimbo.crawler.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class AppConfiguration {

    private Properties properties = new Properties();

    public AppConfiguration() throws IOException {
        properties.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResource("appConfigs.properties")).getPath()));
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

}
