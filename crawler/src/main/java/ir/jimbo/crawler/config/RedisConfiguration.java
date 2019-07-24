package ir.jimbo.crawler.config;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class RedisConfiguration {
    private List<String> nodes;
    private boolean isStandAlone = false;
    private String password;
    private int expiredTime;
    private String setName;

    public RedisConfiguration() throws IOException {
        Properties properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                "redisConfig.properties")));
        nodes = Arrays.asList(properties.getProperty("redis.url").split("."));
        isStandAlone = Boolean.valueOf(properties.getProperty("redis.standalone"));
        password = properties.getProperty("redis.password");
        expiredTime = Integer.parseInt("cache.expired_time");
        setName = properties.getProperty("cache.domain.set.name");
    }

    public List<String> getNodes() {
        return nodes;
    }

    public boolean isStandAlone() {
        return isStandAlone;
    }

    public String getPassword() {
        return password;
    }

    public int getExpiredTime() {
        return expiredTime;
    }

    public String getSetName() {
        return setName;
    }
}
