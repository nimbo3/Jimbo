package ir.jimbo.hbasepageprocessor;

import com.codahale.metrics.Counter;
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

    public PageProcessorThread(String hTableName, String hColumnFamily, MetricConfiguration metrics) throws IOException {
        hTableManager = new HTableManager(hTableName, hColumnFamily,"HBaseHealthChecker", metrics);
        // TODO inject healthChecker name above
        KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
        pageConsumer = kafkaConfiguration.getPageConsumer();
        this.setName("hbase-page processor thread");
        pollDuration = Long.parseLong(kafkaConfiguration.getPropertyValue("consumer.poll.duration"));
        this.metrics = metrics;
    }

    @Override
    public void run() {
        Timer insertHBaseTimer = metrics.getNewTimer("insertHBaseTimer");
        Counter linksCounter = metrics.getNewCounter("linksCounter");
        while (!interrupted()) {
            try {
                ConsumerRecords<Long, Page> records = pageConsumer.poll(Duration.ofMillis(pollDuration));
                Timer.Context pagesProcessDurationContext = insertHBaseTimer.time();
                for (ConsumerRecord<Long, Page> record : records) {
                    Timer.Context oneInsertContext = insertHBaseTimer.time();
                    Page page = record.value();
                    for (HtmlTag link : page.getLinks()) {
                        final String href = link.getProps().get("href");
                        if (href != null && !href.isEmpty())
                            links.add(new HRow(href, page.getUrl(), link.getContent()));
                    }
                    long hbaseInsertDuration = oneInsertContext.stop();
                    ///////
                    LOGGER.info("time passed for processing one page : {}", hbaseInsertDuration);
                }
                hTableManager.put(links);
                LOGGER.info("time taken to process {} given pages from kafka : {}", records.count(), pagesProcessDurationContext.stop());
                linksCounter.inc(links.size());
                links.clear();
                LOGGER.info("number of links: " + linksCounter.getCount());
            } catch (IOException e) {
                LOGGER.error("IO error in pageProcessor thread", e);
            }
        }
    }

    public void close() throws IOException {
        hTableManager.close();
    }
}