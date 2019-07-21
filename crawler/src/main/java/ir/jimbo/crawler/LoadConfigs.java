package ir.jimbo.crawler;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/**
 * load configurations for this project. configs like :
 *     . expired_date (time in seconds) of urls in redis
 *     . thread pools init size
 *     .
 */
public class LoadConfigs {

    private Properties properties = new Properties();

    LoadConfigs() throws IOException {
        properties.load(new FileInputStream(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().
                getResource("configs.properties")).getPath()));
    }

    private String getPropertyValue(String key) {
        return properties.getProperty(key);
    }
}
