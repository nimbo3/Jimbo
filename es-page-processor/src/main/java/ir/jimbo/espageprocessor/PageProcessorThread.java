package ir.jimbo.espageprocessor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.commons.model.Page;
import ir.jimbo.espageprocessor.config.KafkaConfiguration;
import ir.jimbo.espageprocessor.manager.ElasticSearchService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PageProcessorThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageProcessorThread.class);
    private Consumer<Long, Page> pageConsumer;
    private ElasticSearchService esService;
    private Long pollDuration;
    private MetricConfiguration metrics;

    public PageProcessorThread(ElasticSearchService esService, MetricConfiguration metrics) {
        KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
        pageConsumer = kafkaConfiguration.getPageConsumer();
        this.esService = esService;
        this.setName("es-page processor thread");
        pollDuration = Long.parseLong(kafkaConfiguration.getPropertyValue("consumer.poll.duration"));
        this.metrics = metrics;
    }

    @Override
    public void run() {
        Counter pageCounter = metrics.getNewCounter(metrics.getProperty("elastic.pages.counter.name"));
        Timer processTime = metrics.getNewTimer(metrics.getProperty("elastic.process.timer.name"));
        while (!interrupted()) {
            try {
                ConsumerRecords<Long, Page> records = pageConsumer.poll(Duration.ofMillis(pollDuration));
                List<Page> pages = new ArrayList<>();
                Timer.Context timerContext = processTime.time();
                for (ConsumerRecord<Long, Page> record : records) {
                    pages.add(record.value());
                }
                pageCounter.inc(pages.size());
                boolean isAdded = esService.insertPages(pages);
                LOGGER.info("number of pages: " + pageCounter.getCount());
                if (!isAdded)
                    LOGGER.info("ES insertion failed.");
                timerContext.stop();
                LOGGER.info("record_size: " + records.count());
            } catch (Exception e) {
                LOGGER.error("error in process messages", e);
            }
        }
    }
}
