package ir.jimbo.shuffler;

import com.codahale.metrics.Histogram;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.shuffler.config.AppConfig;
import ir.jimbo.shuffler.config.KafkaConfiguration;
import ir.jimbo.commons.exceptions.NoDomainFoundException;
import ir.jimbo.shuffler.model.Link;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Shuffler extends Thread{

    private KafkaConfiguration kafkaConfiguration;
    private AppConfig appConfig;
    private AtomicBoolean repeat;
    private Consumer<Long, String> linkConsumer;
    private Producer<Long, String> linkProducer;
    private MetricConfiguration metricConfiguration;
    private int skipStep;
    private static final Logger LOGGER = LogManager.getLogger(Shuffler.class);
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    // Regex pattern to extract domain from URL
    // Please refer to RFC 3986 - Appendix B for more information

    public Shuffler(AppConfig appConfig) throws IOException {
        this.appConfig = appConfig;
        kafkaConfiguration = new KafkaConfiguration();
        metricConfiguration = MetricConfiguration.getInstance();
    }

    @Override
    public void run() {
        repeat = new AtomicBoolean(true);
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
        while (repeat.get()) {
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
                LOGGER.info("url readed from kafka : {}", url);
                try {
                    links.add(new Link(new StringBuilder(getDomain(url)).reverse().toString(), url));
                } catch (NoDomainFoundException e) {
                    LOGGER.error("cant extract domain from url {}", url, e);
                }
            }
//            try {
//                linkConsumer.commitSync();
//            } catch (Exception e) {
//                LOGGER.error("an error occurred during commit", e);
//            }
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
        LOGGER.debug("start producing links.lists size : {}", size);
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
                    try {
                        Thread.sleep(appConfig.getSleepDuration());
                    } catch (Exception e) {
                        LOGGER.error("error in sleeping", e);
                    }
                }
            }
        }
        LOGGER.info("end sending links to kafka");
    }

    String getDomain(String url) {
        final Matcher matcher = domainPattern.matcher(url);
        String result = null;
        if (matcher.matches())
            result = matcher.group(4);
        if (result == null) {
            throw new NoDomainFoundException();
        }

        if (result.startsWith("www.")) {
            result = result.substring(4);
        }
        if (result.isEmpty()) {
            throw new NoDomainFoundException();
        }
        return result;
    }

    private void sendLink(String link) {
        ProducerRecord<Long, String> record = new ProducerRecord<>(kafkaConfiguration.getShuffledLinksTopicName()
                , (long) link.hashCode(), link);
        linkProducer.send(record);
    }

    @Override
    public void interrupt() {
        LOGGER.info("start closing shuffling app");
        repeat.set(false);
        LOGGER.info("setting repeat to false");
        linkConsumer.close();
        linkProducer.close();
        LOGGER.info("producer and consumer closed");
    }
}
