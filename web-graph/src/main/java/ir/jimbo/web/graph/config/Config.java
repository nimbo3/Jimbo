package ir.jimbo.web.graph.config;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public abstract class Config {
    private Properties properties = new Properties();
    private String prefix;

    Config(String prefix) throws IOException {
        this.prefix = prefix;
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                "configs.properties")));
    }

    public String getPropertyValue(String key) {
        return properties.getProperty(prefix + "." + key);
    }
}