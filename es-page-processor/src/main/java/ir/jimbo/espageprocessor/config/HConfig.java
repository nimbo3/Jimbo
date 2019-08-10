package ir.jimbo.espageprocessor.config;

import java.io.IOException;

public class HConfig extends Config {
    private static final String PREFIX = "hbase";

    private static HConfig instance = null;

    private HConfig() throws IOException {
        super(PREFIX);
    }

    public static HConfig getInstance() throws IOException {
        if (instance == null)
            instance = new HConfig();
        return instance;
    }
}
