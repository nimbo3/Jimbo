package ir.jimbo.crawler;

import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.service.CacheService;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LinkConsumer extends Thread {

    private Logger logger = LogManager.getLogger(this.getClass());
    private long pollDuration;
    private KafkaConfiguration kafkaConfiguration;
    private CacheService cacheService;
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    // Regex pattern to extract domain from URL
    // Please refer to RFC 3986 - Appendix B for more information

    LinkConsumer(KafkaConfiguration kafkaConfiguration, CacheService cacheService) {
        pollDuration = kafkaConfiguration.getPollDuration();
        this.kafkaConfiguration = kafkaConfiguration;
        this.cacheService = cacheService;
    }

    @Override
    public void run() {
        Consumer<Long, String> consumer = kafkaConfiguration.getConsumer();
        String uri;
        Producer<Long, String> producer = kafkaConfiguration.getLinkProducer();
        while (! interrupted()) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(pollDuration));
//            logger.info("get link from kafka numbers taken : " + consumerRecords.count() + consumer.listTopics());
            for (ConsumerRecord<Long, String> record : consumerRecords) {
                uri = record.value();
                logger.debug("the link readed from kafka : " + uri);
                try {
                    if (politenessChecker(getDomain(uri))) {
                        App.linkQueue.put(uri);
                        System.out.println(uri);
                        try {
                            cacheService.addDomain(getDomain(uri));
                            logger.info("uri \"" + uri + "\" added to queue");
                        } catch (NoDomainFoundException e) {
                            logger.error("cant extract domain in PageParserThread from uri : " + uri, e);
                        }
                    } else {
                        logger.info("it was not polite crawling this uri : " + uri + "\n\n\n");
                        ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(
                                kafkaConfiguration.getLinkTopicName(), uri);
                        producer.send(producerRecord);
                    }
                } catch (NoDomainFoundException e) {
                    logger.error("bad uri. cant take domain", e);
                } catch (Exception e) {
                    logger.error("error in putting uri to queue (interrupted exception) uri : " + uri, e);
                    ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(
                            kafkaConfiguration.getLinkTopicName(), uri);
                    producer.send(producerRecord);
                }
            }
            consumer.commitSync();
        }
        producer.close();
        consumer.close();
    }

    private boolean politenessChecker(String uri) {
        return ! cacheService.isDomainExist(uri);
    }

    private String getDomain(String url) {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }
}