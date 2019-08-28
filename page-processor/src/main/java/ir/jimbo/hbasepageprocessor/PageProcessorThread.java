package ir.jimbo.hbasepageprocessor;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import ir.jimbo.hbasepageprocessor.assets.HRow;
import ir.jimbo.hbasepageprocessor.config.KafkaConfiguration;
import ir.jimbo.hbasepageprocessor.manager.HTableManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class PageProcessorThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageProcessorThread.class);
    private final HTableManager hTableManager;
    private Consumer<Long, Page> pageConsumer;
    private Long pollDuration;
    private List<HRow> links = new ArrayList<>();
    private MetricConfiguration metrics;


    public PageProcessorThread(String hTableName, String hColumnFamily, MetricConfiguration metrics) throws IOException, NoSuchAlgorithmException {
        hTableManager = new HTableManager(hTableName, hColumnFamily,"HBaseHealthChecker", metrics);
        KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
        pageConsumer = kafkaConfiguration.getPageConsumer();
        this.setName("hbase-page processor thread");
        pollDuration = Long.parseLong(kafkaConfiguration.getPropertyValue("consumer.poll.duration"));
        this.metrics = metrics;
    }

    @Override
    public void run() {
        Timer insertHBaseTimer = metrics.getNewTimer(metrics.getProperty("hbase.record.process.timer.name"));
        Histogram histogram = metrics.getNewHistogram(metrics.getProperty("hbase.links.read.from.kafka.histogram.name"));
        histogram.update(0);
        while (!interrupted()) {
            try {
                ConsumerRecords<Long, Page> records = pageConsumer.poll(Duration.ofMillis(pollDuration));
                Timer.Context pagesProcessDurationContext = insertHBaseTimer.time();
                for (ConsumerRecord<Long, Page> record : records) {
                    Page page = record.value();
                    Timer.Context oneInsertContext = insertHBaseTimer.time();
                    for (HtmlTag link : page.getLinks()) {
                        final String href = link.getProps().get("href");
                        if (href != null && !href.isEmpty())
                            links.add(new HRow(href, page.getUrl(), link.getContent()));
                    }
                    long hBaseInsertDuration = oneInsertContext.stop();
                    LOGGER.info("time passed for processing one page : {}", hBaseInsertDuration);
                }
                hTableManager.put(links);
                final long processDuration = pagesProcessDurationContext.stop();
                LOGGER.info("time taken to process {} given pages from kafka : {}", records.count(), processDuration);
                histogram.update(links.size());
                links.clear();
            } catch (IOException e) {
                LOGGER.error("IO error in pageProcessor thread", e);

            } catch (Exception e) {
                LOGGER.error("", e);

            }
        }
    }

    public void close() throws IOException {
        hTableManager.close();
    }
}