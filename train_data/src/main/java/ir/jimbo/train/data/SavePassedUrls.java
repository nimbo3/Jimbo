package ir.jimbo.train.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SavePassedUrls extends Thread {

    private final Logger logger = LogManager.getLogger(this.getClass());
    private FileWriter fileWriter;
    private AtomicBoolean repeat;
    private static SavePassedUrls savePassedUrls;

    private SavePassedUrls(String filePath) throws IOException {
        fileWriter = new FileWriter(filePath);
        repeat = new AtomicBoolean(true);
    }

    public static SavePassedUrls getInstance(String filePath) throws IOException {
        if (savePassedUrls == null)
            return savePassedUrls = new SavePassedUrls(filePath);
        return savePassedUrls;
    }

    @Override
    public void run() {
        while (repeat.get()) {
            String url = null;
            try {
                url = App.passedUrls.poll(100, TimeUnit.MILLISECONDS);
                if (url != null) {
                    fileWriter.write(url + "\n");
                }
            } catch (IOException e) {
                logger.error("exception in writing url to file, {}", url, e);
            } catch (Exception e) {
                logger.error("exception in poll from queue", e);
            }
        }
    }

    @Override
    public void interrupt() {
        try {
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            logger.error("exception in flush and closing urls file");
        }
        repeat.set(false);
    }
}
