package ir.jimbo.pageprocessor;

import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import ir.jimbo.pageprocessor.config.KafkaConfiguration;
import ir.jimbo.pageprocessor.manager.ElasticSearchService;
import ir.jimbo.pageprocessor.manager.HTableManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class PageProcessorThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageProcessorThread.class);
    private final HTableManager hTableManager;
    private final String hQualifier;
    private Consumer<Long, Page> pageConsumer;
    private Producer<Long, String> linkProducer;
    private KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
    private ElasticSearchService esService;

    private Long pollDuration;

    public PageProcessorThread(String hTableName, String hColumnFamily, String hQualifier, ElasticSearchService esService) throws IOException {
        hTableManager = new HTableManager(hTableName, hColumnFamily);
        pageConsumer = kafkaConfiguration.getPageConsumer();
        linkProducer = kafkaConfiguration.getLinkProducer();
        this.esService = esService;
        this.hQualifier = hQualifier;
        pollDuration = Long.parseLong(kafkaConfiguration.getPropertyValue("consumer.poll.duration"));
    }

    @Override
    public void run() {
        while (!interrupted()) {
            ConsumerRecords<Long, Page> records = pageConsumer.poll(Duration.ofMillis(pollDuration));
            final long currentTimeMillis = System.currentTimeMillis();
            List<Page> pages = new ArrayList<>();
            for (ConsumerRecord<Long, Page> record : records)
                pages.add(record.value());
            for (Page page : pages)
                for (HtmlTag link : page.getLinks()) {
                try {
                    final String href = link.getProps().get("href");
                    if (href != null && !href.isEmpty())
                        hTableManager.put(href, page.getUrl(), link.getContent());
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
                LOGGER.info("All the links in page with URL " + page.getUrl() + " were added to HBase");
            }
            boolean isAdded = esService.insertPages(pages);
            LOGGER.info(System.currentTimeMillis() - currentTimeMillis);
            pageConsumer.commitSync();
        }
    }
}
