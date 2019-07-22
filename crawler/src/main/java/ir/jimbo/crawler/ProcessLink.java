package ir.jimbo.crawler;

public class ProcessLink extends Thread{

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

    private String getDomain(String url) {

    }
}
