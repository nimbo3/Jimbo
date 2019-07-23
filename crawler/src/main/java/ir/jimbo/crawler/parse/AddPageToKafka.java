package ir.jimbo.crawler.parse;

import ir.jimbo.commons.model.Page;
import ir.jimbo.crawler.Parsing;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.crawler.kafka.PageProducer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AddPageToKafka extends Parsing implements Runnable {

    private String url;
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    private PageProducer producer;
    private String urlsTopicName;
    private String pagesTopicName;

    public AddPageToKafka(PageProducer producer, String urlsTopicName, String pagesTopicName) {
        this.producer = producer;
        this.urlsTopicName = urlsTopicName;
        this.pagesTopicName = pagesTopicName;
    }

    public AddPageToKafka(String url) {
        this.url = url;
    }

    public AddPageToKafka() {
        this.url = "";
    }

    @Override
    public void run() {
        boolean repeat = true;
        while (repeat) {
            try {
                this.url = urlToParseQueue.take();
            } catch (InterruptedException e) {
                repeat = false;
                e.printStackTrace();
            }
            Page page = new PageParser(this.url).parse();
            producer.addPageToKafka(pagesTopicName, page);
            redis.addDomainInDb(getDomain(url));
        }
    }

    private String getDomain(String url) throws NoDomainFoundException {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }
}
