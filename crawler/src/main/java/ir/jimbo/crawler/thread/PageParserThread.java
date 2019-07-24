package ir.jimbo.crawler.thread;

import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.crawler.service.CacheService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageParserThread extends Thread{

    private Logger logger = LogManager.getLogger(this.getClass());
    private LinkedBlockingQueue<String> queue;
    private KafkaConfiguration kafkaConfiguration;
    private CacheService cacheService;
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");

    public PageParserThread(LinkedBlockingQueue<String> queue,
                            KafkaConfiguration kafkaConfiguration, CacheService cacheService) {
        this.queue = queue;
        this.kafkaConfiguration = kafkaConfiguration;
        this.cacheService = cacheService;
    }

    // For Test
    public PageParserThread() {

    }

    @Override
    public void run() {
        Producer<Long, Page> producer = kafkaConfiguration.getPageProducer();
        while (! interrupted()) {
            String uri = null;
            try {
                uri = queue.take();
            } catch (Exception e) {
                logger.error("interrupt exception in page parser",e);
            }
            if (uri == null)
                continue;
            Page page = parse(uri);
            ProducerRecord<Long, Page> record = new ProducerRecord<>(kafkaConfiguration.getPageTopicName(),
                    page);
            producer.send(record);
            try {
                cacheService.addDomain(getDomain(uri));
            } catch (NoDomainFoundException e) {
                logger.error("cant extract domain in PageParserThread", e);
            }
            logger.info("page added to kafka, domain added to redis");
        }
    }

    private String getDomain(String url) {
        final Matcher matcher = domainPattern.matcher(url);
        if (matcher.matches())
            return matcher.group(4);
        throw new NoDomainFoundException();
    }

    private Page parse(String url) { // TODO refactor this function
        logger.info("start parsing...");
        Document document;
        Page page = new Page();
        try {
            document = Jsoup.connect(url).get();
        } catch (IOException e) {
            logger.error("exception in connection to url. empty page instance returned", e);
            return page;
        }

        for (Element element : document.getAllElements()) {
            Set<String> h3to6Tags = new HashSet<>(Arrays.asList("h3", "h4", "h5", "h6"));
            Set<String> plainTextTags = new HashSet<>(Arrays.asList("p", "span", "pre"));
            String text = element.text();
            if (text == null)
                text = "";
            if (h3to6Tags.contains(element.tagName().toLowerCase()))
                page.getH3to6List().add(new HtmlTag(element.tagName(), text));
            else if (plainTextTags.contains(element.tagName().toLowerCase()))
                page.getPlainTextList().add(new HtmlTag(element.tagName(), text));
            else if (element.tagName().equalsIgnoreCase("h1"))
                page.getH1List().add(new HtmlTag("h1", text));
            else if (element.tagName().equalsIgnoreCase("h2"))
                page.getH2List().add(new HtmlTag("h2", text));
            else if (element.tagName().equalsIgnoreCase("title"))
                page.setTitle(text);
            else if (element.tagName().equalsIgnoreCase("a")) {
                String href = element.attr("abs:href");
                if (href == null)
                    href = "";
                HtmlTag linkTag = new HtmlTag("a", text);
                linkTag.getProps().put("href", href);
                page.getLinks().add(linkTag);
            } else if (element.tagName().equalsIgnoreCase("meta")) {
                String name = element.attr("name");
                if (name == null)
                    name = "";
                String content = element.attr("content");
                if (content == null)
                    content = "";
                HtmlTag metaTag = new HtmlTag("meta");
                metaTag.getProps().put("name",name);
                metaTag.getProps().put("content", content);
                page.getMetadata().add(metaTag);
            }
        }
        logger.info("parsing page done.");
        return page;
    }
}
