package ir.jimbo.crawler;

import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.crawler.kafka.PageProducer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessLink extends Parsing {

    private String url;
    private RedisConnection redis;
    private PageProducer producer;
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");

    public ProcessLink(String url) {
        this.url = url;
    }

    public ProcessLink init(RedisConnection redis, PageProducer producer) {
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
                urlToParseQueue.put(url);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
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
