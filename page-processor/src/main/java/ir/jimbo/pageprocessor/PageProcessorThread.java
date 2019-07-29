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
import java.util.concurrent.atomic.AtomicInteger;

public class PageProcessorThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageProcessorThread.class);
    private static AtomicInteger count = new AtomicInteger();
    private final HTableManager hTableManager;
    private Consumer<Long, Page> pageConsumer;
    private ElasticSearchService esService;
    private Long pollDuration;

    public PageProcessorThread(String hTableName, String hColumnFamily, ElasticSearchService esService) throws IOException {
        hTableManager = new HTableManager(hTableName, hColumnFamily);
        KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
        pageConsumer = kafkaConfiguration.getPageConsumer();
        this.esService = esService;
        this.setName("page processor thread");
        pollDuration = Long.parseLong(kafkaConfiguration.getPropertyValue("consumer.poll.duration"));
    }

    @Override
    public void run() {
        while (!interrupted()) {
            try {
                final long currentTimeMillis = System.currentTimeMillis();
                ConsumerRecords<Long, Page> records = pageConsumer.poll(Duration.ofMillis(pollDuration));
                List<Page> pages = new ArrayList<>();
                for (ConsumerRecord<Long, Page> record : records) {
                    pages.add(record.value());
                    Page page = record.value();
//                    HashSet<String> check = new HashSet<>();
//                    if (check.contains(page.getUrl())) {
//                        LOGGER.info("we have duplicated" + page.getUrl());
//                    }
//                    check.add(page.getUrl());
                    for (HtmlTag link : page.getLinks()) {
                        final String href = link.getProps().get("href");
                        if (href != null && !href.isEmpty())
                            hTableManager.put(href, page.getUrl(), link.getContent());
                    }
                }
                count.getAndAdd(pages.size());
                boolean isAdded = esService.insertPages(pages);
                LOGGER.info("number of pages: " + count.get());
                if (!isAdded)
                    LOGGER.info("ES insertion failed.");
                LOGGER.info(System.currentTimeMillis() - currentTimeMillis + " record_size: " + records.count());
            } catch (Exception e) {
                LOGGER.error("error in process messages", e);
            }
        }
    }
}
