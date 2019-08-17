package ir.jimbo.shuffler;


import java.io.IOException;

import ir.jimbo.shuffler.config.AppConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static Thread[] shufflers;
    public static void main( String[] args ) throws IOException {
        LOGGER.info("start Shuffling app");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Thread shuffler : shufflers) {
                shuffler.interrupt();
            }
        }));
        AppConfig appConfig = new AppConfig();
        shufflers = new Thread[appConfig.getThreadsNum()];
        for (int i = 0; i < appConfig.getThreadsNum(); i++) {
            shufflers[i] = new Shuffler(appConfig);
            shufflers[i].start();
        }
    }
}
