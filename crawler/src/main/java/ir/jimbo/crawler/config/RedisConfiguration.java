package ir.jimbo.crawler.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class RedisConfiguration {
    private List<String> nodes;
    private boolean isStandAlone;
    private String password;
    private int domainExpiredTime;
    private int urlExpiredTime;

    public RedisConfiguration() throws IOException {
        Properties properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                "redisConfig.properties")));
        initValues(properties);
    }

    public RedisConfiguration(String path) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(path));
        initValues(properties);
    }

    private void initValues(Properties properties) {
        nodes = Arrays.asList(properties.getProperty("redis.url").split(","));
        isStandAlone = Boolean.parseBoolean(properties.getProperty("redis.standalone"));
        password = properties.getProperty("redis.password");
        urlExpiredTime = Integer.parseInt(properties.getProperty("cache.url_expired_time"));
        domainExpiredTime = Integer.parseInt(properties.getProperty("cache.expired_time"));
    }

    public List<String> getNodes() {
        return nodes;
    }

    boolean isStandAlone() {
        return isStandAlone;
    }

    public String getPassword() {
        return password;
    }

    public int getDomainExpiredTime() {
        return domainExpiredTime;
    }

    public int getUrlExpiredTime() {
        return urlExpiredTime;
    }
}
