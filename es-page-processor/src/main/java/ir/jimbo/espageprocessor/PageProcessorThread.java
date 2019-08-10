package ir.jimbo.espageprocessor;

import com.codahale.metrics.Histogram;
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

public class PageProcessorThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageProcessorThread.class);
    private Consumer<Long, Page> pageConsumer;
    private ElasticSearchService esService;
    private Long pollDuration;
    private MetricConfiguration metrics;
    private int numberOfRetry = 10;

    public PageProcessorThread(ElasticSearchService esService, MetricConfiguration metrics, int numberOfRetry) {
        KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
        pageConsumer = kafkaConfiguration.getPageConsumer();
        this.esService = esService;
        this.setName("es-page processor thread");
        pollDuration = Long.parseLong(kafkaConfiguration.getPropertyValue("consumer.poll.duration"));
        this.metrics = metrics;
        this.numberOfRetry = numberOfRetry;
    }

    @Override
    public void run() {
        Histogram histogram = metrics.getNewHistogram(metrics.getProperty("elastic.pages.histogram.name"));
        Timer processTime = metrics.getNewTimer(metrics.getProperty("elastic.process.timer.name"));
        histogram.update(0);
        while (!interrupted()) {
            try {
                ConsumerRecords<Long, Page> records = pageConsumer.poll(Duration.ofMillis(pollDuration));
                List<Page> pages = new ArrayList<>();
                Timer.Context timerContext = processTime.time();
                for (ConsumerRecord<Long, Page> record : records) {
                    pages.add(record.value());
                }
                histogram.update(pages.size());
                boolean isAdded = false;
                int retryCounter = 0;
                while (!isAdded && retryCounter < numberOfRetry) {
                    isAdded = esService.insertPages(pages);
                    if (!isAdded) {
                        retryCounter++;
                        Thread.sleep(100);
                        LOGGER.info("ES insertion failed.");
                    }
                }
                timerContext.stop();
                LOGGER.info("record_size: " + records.count());
            } catch (Exception e) {
                LOGGER.error("error in process messages", e);
            }
        }
        pageConsumer.close();
    }
}
