package ir.jimbo.crawler.parse;

import ir.jimbo.commons.model.Page;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AddPageToKafka extends Parser implements Runnable {

    private Logger logger = LogManager.getLogger(this.getClass());
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");

    public AddPageToKafka() {
    }

    @Override
    public void run() {
        boolean repeat = true;
        Producer<Long, Page> producer = kafkaConfiguration.getPageProducer();
        while (repeat) {
            String url = "";
            try {
                url = urlToParseQueue.take();
            } catch (InterruptedException e) {
                logger.error("exception in taking url from blocking queue. stoping thread : "
                        + Thread.currentThread().getName(), e);
                repeat = false;
            }
            Page page = new PageParser(url).parse();
            ProducerRecord<Long, Page> record = new ProducerRecord<>(kafkaConfiguration.getProperty("pages.topic.name"),
                    page);
            producer.send(record);
            redis.addDomain(getDomain(url));
            logger.info("page added to kafka, domain added to redis");
        }
        logger.info("starting a new Thread because thread " + Thread.currentThread().getName() + " was stopped");
        // start a thread
    }

    private String getDomain(String url) throws NoDomainFoundException {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }
}
