package ir.jimbo.web.graph;


import ir.jimbo.web.graph.config.AppConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main( String[] args ) throws IOException, NoSuchAlgorithmException {
        LOGGER.info("starting web-graph module");
        new WebGraph(new AppConfiguration()).start();
    }
}