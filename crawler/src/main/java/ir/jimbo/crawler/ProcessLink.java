package ir.jimbo.crawler;

import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessLink extends Parser {

    private Logger logger = LogManager.getLogger(this.getClass());
    private String url;
    private CacheService redis;
    private KafkaConfiguration kafkaConfiguration;
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    //Please refer to RFC 3986 - Appendix B for more information

    ProcessLink(String url, RedisConnection redis, KafkaConfiguration kafkaConfiguration) {
        this.url = url;
        this.redis = redis;
        this.kafkaConfiguration = kafkaConfiguration;
    }

    public void process() {
        String domain;
        try {
            domain = getDomain(url);
        } catch (NoDomainFoundException e) {
            return;
        }
        if (!redis.isDomainExist(domain)) {
            try {
                logger.info("add a url to blocking queue started... waiting for blocking queue for place");
                urlToParseQueue.put(url);
                logger.info("add url to blocking queue ended.");
            } catch (InterruptedException e) {
                logger.error("exception in adding url to blocking queue", e);
            }
        } else {
            logger.info("add link to kafka again because already crawled");
            Producer<Long, String> producer = kafkaConfiguration.getLinkProducer();
            ProducerRecord<Long, String> record = new ProducerRecord<>(kafkaConfiguration.getProperty("links.topic.name"),
                    url);
            producer.send(record);
        }

    }

    private String getDomain(String url) throws NoDomainFoundException {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }

}
