package ir.jimbo.crawler.config;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class KafkaConfiguration {

    private Properties properties = new Properties();

    public KafkaConfiguration() throws IOException {
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("kafkaConfig.properties")));
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

}
