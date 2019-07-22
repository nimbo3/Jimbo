package ir.jimbo.pageprocessor.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class HConfig extends Config {
    private static final Logger LOGGER = LogManager.getLogger(HConfig.class);
    private static final String PREFIX = "hbase";

    private static HConfig instance;

    static {
        try {
            instance = new HConfig();
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }

    private HConfig() throws IOException {
        super(PREFIX);
    }

    public static HConfig getInstance() {
        return instance;
    }
}
