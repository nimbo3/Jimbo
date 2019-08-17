package ir.jimbo.config;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class AppConfig {

    private int linksPerProcessSize;
    private int pollAttempts;
    private int skipStep;

    public AppConfig() throws IOException {
        Properties properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("configs.properties")));
        initValues(properties);
    }

    private void initValues(Properties properties) {
        linksPerProcessSize = Integer.parseInt(properties.getProperty("links.per.process.size"));
        pollAttempts = Integer.parseInt(properties.getProperty("max.poll.attempt"));
        skipStep = Integer.parseInt(properties.getProperty("links.skip.step"));
    }

    public int getLinksPerProcessSize() {
        return linksPerProcessSize;
    }

    public int getPollAttempts() {
        return pollAttempts;
    }

    public int getSkipStep() {
        return skipStep;
    }
}
