package ir.jimbo.crawler;

import ir.jimbo.commons.model.TitleAndLink;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.crawler.kafka.MyProducer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessLink extends PageParse {

    private String title;
    private String url;
    private RedisConnection redis;
    private MyProducer producer;
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");

    public ProcessLink(String title, String url) {
        this.title = title;
        this.url = url;
    }

    public void init(RedisConnection redis, MyProducer producer) {
        this.redis = redis;
        this.producer = producer;
    }

    public void process() {
        String domain = getDomain(url);
        if (!redis.existsDomainInDB(domain)) {
            try {
                urlToParseQueue.put(url);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            producer.addLinkToKafka("links", new TitleAndLink(title, url));
        }
    }

    private String getDomain(String url) throws NoDomainFoundException {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }

    private boolean checkValidUrl(String url) {
        return url.endsWith(".html") || url.endsWith(".htm") || url.endsWith(".php")
                || !url.substring(url.lastIndexOf('/') + 1).contains(".");
    }

}
