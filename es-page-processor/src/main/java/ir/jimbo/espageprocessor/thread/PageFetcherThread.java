package ir.jimbo.espageprocessor.thread;

import com.codahale.metrics.Counter;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.commons.model.Page;
import ir.jimbo.espageprocessor.config.ApplicationConfiguration;
import ir.jimbo.espageprocessor.config.KafkaConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;

public class PageFetcherThread extends Thread {
    private static final Logger LOGGER = LogManager.getLogger(PageFetcherThread.class);
    private Consumer<Long, Page> pageConsumer;
    private Long pollDuration;
    private MetricConfiguration metrics;
    private ArrayBlockingQueue<Page> queue;

    public PageFetcherThread(ApplicationConfiguration appConfig, ArrayBlockingQueue<Page> queue, MetricConfiguration metrics) {
        KafkaConfiguration kafkaConfiguration = KafkaConfiguration.getInstance();
        pageConsumer = kafkaConfiguration.getPageConsumer();
        this.queue = queue;
        this.setName(this.getName() + "es-page processor thread");
        pollDuration = Long.parseLong(kafkaConfiguration.getPropertyValue("consumer.poll.duration"));
        this.metrics = metrics;
    }

    @Override
    public void run() {
        Counter pageCounter = metrics.getNewCounter("espp.pageReadedFromKafkaCounter");
        while (!interrupted()) {
            try {
                ConsumerRecords<Long, Page> records = pageConsumer.poll(Duration.ofMillis(pollDuration));
                pageCounter.inc(records.count());
                for (ConsumerRecord<Long, Page> record : records) {
                    queue.put(record.value());
                }
            } catch (Exception e) {
                LOGGER.error("error in process messages", e);
            }
        }
        pageConsumer.close();
    }
}
