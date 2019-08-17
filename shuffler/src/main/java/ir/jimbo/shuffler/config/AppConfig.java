package ir.jimbo.shuffler.config;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class AppConfig {

    private int linksPerProcessSize;
    private int pollAttempts;
    private int skipStep;
    private String shuffleProcessTimerName;
    private String consumeTimerName;
    private String sortTimerName;
    private String produceTimerName;
    private String listSizeHistogramName;
    private int threadsNum;
    private int sleepDuration;

    public AppConfig() throws IOException {
        Properties properties = new Properties();
        properties.load(Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("configs.properties")));
        initValues(properties);
    }

    private void initValues(Properties properties) {
        linksPerProcessSize = Integer.parseInt(properties.getProperty("links.per.process.size"));
        pollAttempts = Integer.parseInt(properties.getProperty("max.poll.attempt"));
        skipStep = Integer.parseInt(properties.getProperty("links.skip.step"));
        shuffleProcessTimerName = properties.getProperty("shuffle.process.timer.name");
        consumeTimerName = properties.getProperty("consume.timer.name");
        sortTimerName = properties.getProperty("sort.timer.name");
        produceTimerName = properties.getProperty("produce.timer.name");
        listSizeHistogramName = properties.getProperty("list.size.histogram.name");
        threadsNum = Integer.parseInt(properties.getProperty("threads.num"));
        sleepDuration = Integer.parseInt(properties.getProperty("sleep.duration.per.send"));
    }

    public int getLinksPerProcessSize() {
        return linksPerProcessSize;
    }

    public int getPollAttempts() {
        return pollAttempts;
    }

    public int getSkipStep() {
        return skipStep;
    }

    public String getShuffleProcessTimerName() {
        return shuffleProcessTimerName;
    }

    public String getConsumeTimerName() {
        return consumeTimerName;
    }

    public String getSortTimerName() {
        return sortTimerName;
    }

    public String getProduceTimerName() {
        return produceTimerName;
    }

    public String getListSizeHistogramName() {
        return listSizeHistogramName;
    }

    public int getThreadsNum() {
        return threadsNum;
    }

    public int getSleepDuration() {
        return sleepDuration;
    }
}
