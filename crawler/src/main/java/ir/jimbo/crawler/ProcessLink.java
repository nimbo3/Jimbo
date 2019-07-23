package ir.jimbo.crawler;

import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.crawler.kafka.PageAndLinkProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessLink extends Parsing {

    private Logger logger = LogManager.getLogger(this.getClass());
    private String url;
    private RedisConnection redis;
    private PageAndLinkProducer producer;
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    //Please refer to RFC 3986 - Appendix B for more information

    public ProcessLink(String url) {
        this.url = url;
    }

    public ProcessLink init(RedisConnection redis, PageAndLinkProducer producer) {
        this.redis = redis;
        this.producer = producer;
        return this;
    }

    public void process(String linksTopicName) {
        String domain;
        try {
            domain = getDomain(url);
        } catch (NoDomainFoundException e) {
            return;
        }
        if (!redis.existsDomainInDB(domain)) {
            try {
                System.out.println("add url to blocking queue");
                urlToParseQueue.put(url);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("add link to kafka because already crawled");
            producer.addLinkToKafka(linksTopicName, url);
        }

    }

    private String getDomain(String url) throws NoDomainFoundException {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }

}
