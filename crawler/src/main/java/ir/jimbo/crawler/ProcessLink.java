package ir.jimbo.crawler;

import ir.jimbo.commons.model.TitleAndLink;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.crawler.kafka.MyProducer;
import ir.jimbo.crawler.misc.Constants;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessLink extends Thread {

    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");

    String title;
    String url;
    RedisConnection redis;
    MyProducer producer;

    public ProcessLink(String title, String url, RedisConnection redis, MyProducer producer) {
        this.title = title;
        this.url = url;
        this.redis = redis;
        this.producer = producer;
    }

    @Override
    public void run() {
        String domain = getDomain(url);
        if (!redis.existsDomainInDB(domain)) {
            if (checkValidUrl(url)) {
                redis.addDomainInDb(domain, url);
                if (checkRobots(domain)) {
                    // thread poll
                }
            }
        } else {
            producer.addLinkToKafka(Constants.KAFKA_LINKS_TOPIC, new TitleAndLink(title, url));
        }
    }

    private boolean checkRobots(String domain) {
        return false;
    }

    private boolean checkValidUrl(String url) {
        return url.endsWith(".html") || url.endsWith(".htm") || !url.substring(url.lastIndexOf('/') + 1).contains(".");
    }

    private String getDomain(String url) throws NoDomainFoundException {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }
}
