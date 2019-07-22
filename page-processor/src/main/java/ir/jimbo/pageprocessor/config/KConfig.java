package ir.jimbo.pageprocessor.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class KConfig extends Config {
    private static final Logger LOGGER = LogManager.getLogger(HConfig.class);
    private static final String PREFIX = "kafka";

    private static KConfig instance;

    static {
        try {
            instance = new KConfig();
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }

    private KConfig() throws IOException {
        super(PREFIX);
    }

    public static KConfig getInstance() {
        return instance;
    }
}
