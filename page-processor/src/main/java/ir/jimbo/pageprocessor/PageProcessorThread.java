package ir.jimbo.pageprocessor;

import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import ir.jimbo.pageprocessor.config.KafkaConfiguration;
import ir.jimbo.pageprocessor.manager.ElasticSearchService;
import ir.jimbo.pageprocessor.manager.HTableManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PageProcessorThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageProcessorThread.class);
    private final HTableManager hTableManager;
    private Consumer<Long, Page> pageConsumer;
    private ElasticSearchService esService;
    private BlockingQueue<Page> pagesQueue;

    private Long pollDuration;

    public PageProcessorThread(String hTableName, String hColumnFamily, ElasticSearchService esService) throws IOException {
        hTableManager = new HTableManager(hTableName, hColumnFamily);
        KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
        pageConsumer = kafkaConfiguration.getPageConsumer();
        this.esService = esService;
        pollDuration = Long.parseLong(kafkaConfiguration.getPropertyValue("consumer.poll.duration"));
        pagesQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        new Thread(() -> {
            try {
                Page page = pagesQueue.take();
                for (HtmlTag link : page.getLinks()) {
                    final String href = link.getProps().get("href");
                    if (href != null && !href.isEmpty())
                        hTableManager.put(href, page.getUrl(), link.getContent());
                    LOGGER.info("All the links in page with URL " + page.getUrl() + " were added to HBase");
                }
            } catch (InterruptedException | IOException e) {
                LOGGER.error("", e);
            }
        }).start();
        while (!interrupted()) {
            ConsumerRecords<Long, Page> records = pageConsumer.poll(Duration.ofMillis(pollDuration));
            final long currentTimeMillis = System.currentTimeMillis();
            List<Page> pages = new ArrayList<>();
            for (ConsumerRecord<Long, Page> record : records) {
                pages.add(record.value());
                try {
                    pagesQueue.put(record.value());
                } catch (InterruptedException e) {
                    LOGGER.error("", e);
                }
            }
            boolean isAdded = esService.insertPages(pages);
            if (!isAdded)
                LOGGER.info("ES insertion failed.");
            LOGGER.info(System.currentTimeMillis() - currentTimeMillis);
            pageConsumer.commitSync();
        }
    }
}
