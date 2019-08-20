package ir.jimbo.web.graph;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main( String[] args ) throws IOException {
        LOGGER.info("starting web_graph module");
        new WebGraph().start();
    }
}
