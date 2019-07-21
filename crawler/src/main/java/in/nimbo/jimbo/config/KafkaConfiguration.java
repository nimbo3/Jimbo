package in.nimbo.jimbo.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class KafkaConfiguration {

    private Properties properties = new Properties();

    KafkaConfiguration() throws IOException {
        properties.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResource("kafkaConfigs.properties")).getPath()));
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

}
