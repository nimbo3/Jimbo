package ir.jimbo.hbasepageprocessor;

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
import java.util.concurrent.atomic.AtomicInteger;

public class PageProcessorThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageProcessorThread.class);
    private static AtomicInteger count = new AtomicInteger();
    private final HTableManager hTableManager;
    private Consumer<Long, Page> pageConsumer;
    private Long pollDuration;
    private List<HRow> links = new ArrayList<>();

    public PageProcessorThread(String hTableName, String hColumnFamily) throws IOException {
        hTableManager = new HTableManager(hTableName, hColumnFamily);
        KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
        pageConsumer = kafkaConfiguration.getPageConsumer();
        this.setName("hbase-page processor thread");
        pollDuration = Long.parseLong(kafkaConfiguration.getPropertyValue("consumer.poll.duration"));
    }

    @Override
    public void run() {
        while (!interrupted()) {
            try {
                final long currentTimeMillis = System.currentTimeMillis();
                ConsumerRecords<Long, Page> records = pageConsumer.poll(Duration.ofMillis(pollDuration));
                for (ConsumerRecord<Long, Page> record : records) {
                    Page page = record.value();
                    for (HtmlTag link : page.getLinks()) {
                        final String href = link.getProps().get("href");
                        if (href != null && !href.isEmpty())
                            links.add(new HRow(href, page.getUrl(), link.getContent()));
                    }
                }
                hTableManager.put(links);
                links.clear();
                count.getAndAdd(links.size());
                LOGGER.info("number of links: " + count.get());
                LOGGER.info(System.currentTimeMillis() - currentTimeMillis + " record_size: " + records.count());
            } catch (Exception e) {
                LOGGER.error("error in process messages", e);
            }
        }
    }
}
