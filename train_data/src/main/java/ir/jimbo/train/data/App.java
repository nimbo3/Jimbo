package ir.jimbo.train.data;

import ir.jimbo.train.data.config.AppConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.*;

public class App {

    private static final Logger logger = LogManager.getLogger(App.class);
    protected static ArrayBlockingQueue<String> passedUrls;
    public static ThreadPoolExecutor executor;

    public static void main( String[] args ) throws IOException {
        logger.info("app started...");
        AppConfig appConfig = new AppConfig();
        passedUrls = new ArrayBlockingQueue<>(appConfig.getLocalQueueSize());
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(appConfig.getThreadPoolSize());
        SavePassedUrls.getInstance(appConfig.getUrlFilePath()).start();
        logger.info("init app done");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            try {
                SavePassedUrls.getInstance(appConfig.getUrlFilePath()).interrupt();
            } catch (IOException e) {
                logger.error(e);
            }
        }));
        initApp();
    }

    /**
     * Add seed urls with conditions. code example :
     *  <code>
     *      new CrawlProtected("www.varzesh3.com", "sport news", 10, 30000, true, null, null, null);
     *  </code>
     *  parameters in order : url, urlsAnchor, crawlDepth, politenessTimeMillis, stayInDomain, anchorsKeyWord
     *      , contentKeyword, MetaContain
     */
    private static void initApp() {

    }
}
