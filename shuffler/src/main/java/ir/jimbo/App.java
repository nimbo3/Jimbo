package ir.jimbo;


import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    public static void main( String[] args ) throws IOException {
        LOGGER.info("start Shuffling app");
        Shuffler shuffler = new Shuffler();
        shuffler.start();
        Runtime.getRuntime().addShutdownHook(new Thread(shuffler::close));
    }
}
