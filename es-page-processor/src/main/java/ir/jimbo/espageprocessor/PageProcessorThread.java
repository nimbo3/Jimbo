package ir.jimbo.espageprocessor;

import ir.jimbo.commons.model.Page;
import ir.jimbo.espageprocessor.config.KafkaConfiguration;
import ir.jimbo.espageprocessor.manager.ElasticSearchService;
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
    private Consumer<Long, Page> pageConsumer;
    private ElasticSearchService esService;
    private Long pollDuration;

    public PageProcessorThread(ElasticSearchService esService) throws IOException {
        KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
        pageConsumer = kafkaConfiguration.getPageConsumer();
        this.esService = esService;
        this.setName("es-page processor thread");
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
