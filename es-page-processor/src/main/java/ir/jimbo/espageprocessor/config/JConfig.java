package ir.jimbo.espageprocessor.config;

import java.io.IOException;

public class JConfig extends Config {
    private static final String PREFIX = "java";

    private static JConfig instance = null;

    private JConfig() throws IOException {
        super(PREFIX);
    }

    public static JConfig getInstance() throws IOException {
        if (instance == null)
            instance = new JConfig();
        return instance;
    }
}
