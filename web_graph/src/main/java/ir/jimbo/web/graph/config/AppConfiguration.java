package ir.jimbo.web.graph.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class AppConfiguration {

    private Properties properties;
    private int graphNodeNumber;

    public AppConfiguration() throws IOException {
        properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("configs.properties")));
        initValues(properties);
    }

    public AppConfiguration(String path) throws IOException {
        properties = new Properties();
        properties.load(new FileInputStream(path));
        initValues(properties);
    }

    private void initValues(Properties properties) {
        graphNodeNumber = Integer.parseInt(properties.getProperty("graph.node.number"));
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public int getGraphNodeNumber() {
        return graphNodeNumber;
    }
}
