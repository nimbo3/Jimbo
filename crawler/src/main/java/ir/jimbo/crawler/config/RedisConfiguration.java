package ir.jimbo.crawler.config;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class RedisConfiguration {

    private Properties properties = new Properties();

    public RedisConfiguration() throws IOException {
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                "redisConfig.properties")));
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

}
