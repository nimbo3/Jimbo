package ir.jimbo.crawler;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.crawler.service.CacheService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sound.midi.SysexMessage;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LinkConsumer extends Thread {
    private Logger logger = LogManager.getLogger(this.getClass());
    private long pollDuration;
    private KafkaConfiguration kafkaConfiguration;
    private CacheService cacheService;
    private AtomicBoolean repeat;
    private CountDownLatch countDownLatch;

    // If invalid uri go for check its politeness it will return false and with this we make sure that it will not go back to kafka.
    private boolean backToKafka;
    private MetricConfiguration metrics;

    // Regex pattern to extract domain from URL
    //Please refer to RFC 3986 - Appendix B for more information
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");

    private Consumer<Long, String> consumer;
    private ArrayBlockingQueue<String> queue;

    public LinkConsumer(KafkaConfiguration kafkaConfiguration, CacheService cacheService, CountDownLatch consumerLatch,
                        ArrayBlockingQueue<String> queue, MetricConfiguration metrics) {
        pollDuration = kafkaConfiguration.getPollDuration();
        this.kafkaConfiguration = kafkaConfiguration;
        this.cacheService = cacheService;
        repeat = new AtomicBoolean(true);
        countDownLatch = consumerLatch;
        this.metrics = metrics;
        consumer = kafkaConfiguration.getConsumer();
        this.queue = queue;
    }

    public void setConsumer(Consumer<Long, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        String uri;
        Timer linkProcessTimer = metrics.getNewTimer(metrics.getProperty("crawler.link.process.timer.name"));
        Timer crawlKafkaLinksProcessTimer = metrics.getNewTimer(metrics.getProperty("crawler.kafka.links.process.timer.name"));
        Counter linksCounter = metrics.getNewCounter(metrics.getProperty("crawler.links.readed.from.kafka.counter.name"));
        Counter linksAddToQueueCounter = metrics.getNewCounter(metrics.getProperty("crawler.links.added.to.queue.counter.name"));
        Counter numberOfQueueIsFull = metrics.getNewCounter(metrics.getProperty("crawler.queue.is.full.times"));
        Counter notPoliteLinkCounter = metrics.getNewCounter(metrics.getProperty("crawl.not.polite.link.counter.name"));
        Counter seenIn24HoursCounter = metrics.getNewCounter(metrics.getProperty("crawl.seen.in.24.hours.counter.name"));
        Producer<Long, String> producer = kafkaConfiguration.getLinkProducer();
        logger.info("consumer thread started");
        while (repeat.get()) {
            logger.info("start to poll :");
            long start = System.currentTimeMillis();
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(pollDuration));
            logger.info("end poll , time : {}", System.currentTimeMillis() -start);
            Timer.Context bigTimerContext = crawlKafkaLinksProcessTimer.time();
            for (ConsumerRecord<Long, String> record : consumerRecords) {
                uri = record.value();
                logger.info("uri read from kafka : {}", uri);
                linksCounter.inc();
                Timer.Context timerContext = linkProcessTimer.time();
                try {
                    if (!cacheService.isUrlExists(uri)) {
                        backToKafka = true;
                        if (isPolite(uri)) {
                            boolean isAdded;
                            isAdded = queue.offer(uri, 500, TimeUnit.MILLISECONDS);
                            if (isAdded) {
                                logger.info("uri added to queue : {}", uri);
                                linksAddToQueueCounter.inc();
                                cacheService.addDomain(getDomain(uri));
                                cacheService.addUrl(uri);
                                logger.info("uri \"{}\" added to queue", uri);
                                backToKafka = false;
                            } else {
                                numberOfQueueIsFull.inc();
                                logger.info("queue has not space for this url : {}", uri);
                                sendUriToKafka(uri, producer);
                            }
                        } else {
                            logger.info("it was not polite crawling this uri : {}", uri);
                            notPoliteLinkCounter.inc();                        }
                        if (backToKafka) {
                            sendUriToKafka(uri, producer);
                        }
                    } else {
                        logger.info("this uri was saved in past 24 hours: {}", uri);
                        seenIn24HoursCounter.inc();
                    }
                } catch (NoDomainFoundException e) {
                    logger.error("bad uri. cant take domain", e);
                } catch (Exception e) {
                    logger.error("error in putting uri to queue (interrupted exception) uri : {}", uri);
                    if (!cacheService.isUrlExists(uri)) {
                        sendUriToKafka(uri, producer);
                    }
                }
                timerContext.stop();
            }
//            try {
//                consumer.commitSync();
//            } catch (Exception e) {
//                logger.info("unable to commit.###################################################", e);
//            }
            bigTimerContext.stop();
        }
        logger.info("consumer countdown latch before");
        countDownLatch.countDown();
        logger.info("consumer count down latch. size = {}", countDownLatch.getCount());
        try {
            producer.close();
            consumer.close();
        } catch (Exception e) {
            logger.error("error in closing producer", e);
        }
    }

    private void sendUriToKafka(String uri, Producer<Long, String> producer) {
        ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(
                kafkaConfiguration.getLinkTopicName(), uri);
        producer.send(producerRecord);
    }

    private boolean isPolite(String uri) {
        try {
            return !cacheService.isDomainExist(getDomain(uri));
        } catch (NoDomainFoundException e) {
            backToKafka = false;
            return false;
        }
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

    @Override
    public void interrupt() {
        logger.info("setting repeat to false");
        repeat.set(false);
        logger.info("repeat : {}", repeat.get());
    }
}
