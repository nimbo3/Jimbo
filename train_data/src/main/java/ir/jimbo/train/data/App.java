package ir.jimbo.train.data;

import java.io.FileNotFoundException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class App {

    protected static ArrayBlockingQueue<String> passedUrls;

    public static void main( String[] args ) throws FileNotFoundException {
        passedUrls = new ArrayBlockingQueue<>(100);
        SavePassedUrls.getInstance(new AtomicBoolean(true)).start();
        new CrawlProtected().start();
    }
}
