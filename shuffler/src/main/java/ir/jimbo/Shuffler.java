package ir.jimbo;

import com.codahale.metrics.Histogram;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.config.AppConfig;
import ir.jimbo.config.KafkaConfiguration;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.model.Link;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.codahale.metrics.Timer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class Shuffler {

    private KafkaConfiguration kafkaConfiguration;
    private AppConfig appConfig;
    private boolean repeat;
    private Consumer<Long, String> linkConsumer;
    private Producer<Long, String> linkProducer;
    private MetricConfiguration metricConfiguration;
    private int skipStep;
    private static final Logger LOGGER = LogManager.getLogger(Shuffler.class);
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    // Regex pattern to extract domain from URL
    // Please refer to RFC 3986 - Appendix B for more information

    public Shuffler() throws IOException {
        kafkaConfiguration = new KafkaConfiguration();
        appConfig = new AppConfig();
        metricConfiguration = MetricConfiguration.getInstance();
    }

    void start() {
        repeat = true;
        LOGGER.info("creating kafka consumer and producer");
        linkConsumer = kafkaConfiguration.getConsumer();
        linkProducer = kafkaConfiguration.getLinkProducer();
        skipStep = appConfig.getSkipStep();
        LOGGER.info("creating metrics");
        Timer wholeTime = metricConfiguration.getNewTimer(appConfig.getShuffleProcessTimerName());
        Timer consumeTimer = metricConfiguration.getNewTimer(appConfig.getConsumeTimerName());
        Timer sortTimer = metricConfiguration.getNewTimer(appConfig.getSortTimerName());
        Timer produceTimer = metricConfiguration.getNewTimer(appConfig.getProduceTimerName());
        Histogram listSizeHistogram = metricConfiguration.getNewHistogram(appConfig.getListSizeHistogramName());
        LOGGER.info("starting process...");
        while (repeat) {
            Timer.Context wholeTimeContext = wholeTime.time();
            Timer.Context consumeTimerContext = consumeTimer.time();
            List<Link> links = consumeLinks();
            listSizeHistogram.update(links.size());
            consumeTimerContext.stop();
            Timer.Context sortTimerContext = sortTimer.time();
            sortLinks(links);
            sortTimerContext.stop();
            Timer.Context produceTimerContext = produceTimer.time();
            shuffleLinks(links);
            produceTimerContext.stop();
            wholeTimeContext.stop();
        }
    }

    private List<Link> consumeLinks() {
        int attempt = 0;
        int size = 0;
        String url;
        List<Link> links = new ArrayList<>();
        LOGGER.info("start consuming links...");
        while (size < appConfig.getLinksPerProcessSize()) {
            ConsumerRecords<Long, String> consumerRecords = linkConsumer.poll(Duration.ofMillis(kafkaConfiguration.getPollDuration()));
            size += consumerRecords.count();
            LOGGER.info("links consumed to now : {}", size);
            for (ConsumerRecord<Long, String> consumerRecord : consumerRecords) {
                url = consumerRecord.value();
                try {
                    links.add(new Link(getDomain(url), url));
                } catch (NoDomainFoundException e) {
                    LOGGER.error("cant extract domain from url {}", url, e);
                }
            }
            try {
                linkConsumer.commitSync();
            } catch (Exception e) {
                LOGGER.error("an error occurred during commit", e);
            }
            attempt ++;
            if (attempt > appConfig.getPollAttempts()) {
                LOGGER.warn("maximum number of poll attempts reached. breaking from loop");
                break;
            }
        }
        return links;
    }

    private void sortLinks(List<Link> links) {
        links.sort(Comparator.comparing(Link::getDomain));
    }

    private void shuffleLinks(List<Link> links) {
        int index = 0;
        boolean flag = true;
        int size = links.size();
        LOGGER.info("start producing links.lists size : {}", size);
        while (size != 0) {
            sendLink(links.get(index).getUrl());
            links.remove(index);
            size -= 1;
            index += skipStep;
            if (index >= size) {
                if (flag) {
                    index = size - 1;
                    flag = false;
                } else {
                    index = 0;
                    flag = true;
                }
            }
        }
        LOGGER.info("end sending links to kafka");
    }

    private void sendLink(String link) {
        ProducerRecord<Long, String> record = new ProducerRecord<>(kafkaConfiguration.getShuffledLinksTopicName(), link);
        linkProducer.send(record);
    }

    public void close() {
        LOGGER.info("start closing shuffling app");
        repeat = false;
        LOGGER.info("setting repeat to false");
        linkConsumer.close();
        linkProducer.close();
        LOGGER.info("producer and consumer closed");
    }
}
