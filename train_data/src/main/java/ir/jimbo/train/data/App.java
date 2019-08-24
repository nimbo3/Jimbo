package ir.jimbo.train.data;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class App {

    protected static ArrayBlockingQueue<String> passedUrls;
    public static ThreadPoolExecutor executor;

    public static void main( String[] args ) throws IOException {
        passedUrls = new ArrayBlockingQueue<>(500);
        SavePassedUrls.getInstance(new AtomicBoolean(true)).start();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(100);
        ReadUrls.getInstance().start();
    }
}
