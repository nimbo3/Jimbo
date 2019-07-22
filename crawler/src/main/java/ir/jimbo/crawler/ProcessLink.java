package ir.jimbo.crawler;

import ir.jimbo.crawler.exceptions.NoDomainFoundException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessLink extends Thread{

    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");

    String title;
    String url;
    RedisConnection redis;

    public ProcessLink(String title, String url, RedisConnection redis) {
        this.title = title;
        this.url = url;
        this.redis = redis;
    }

    @Override
    public void run() {
        String domain = getDomain(url);
        if (! redis.existsDomainInDB(domain)) {
            if (checkValid(url)) {
                redis.addDomainInDb(domain, url);
            }
        }
    }

    private boolean checkValid(String url) {
        return false;
    }

    private String getDomain(String url) throws NoDomainFoundException {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }
}
