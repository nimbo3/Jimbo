package ir.jimbo.crawler.thread;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import ir.jimbo.crawler.App;
import ir.jimbo.crawler.config.KafkaConfiguration;
import ir.jimbo.crawler.service.CacheService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PageParserThread extends Thread {

    private Logger logger = LogManager.getLogger(this.getClass());
    private ArrayBlockingQueue<String> queue;
    private KafkaConfiguration kafkaConfiguration;
    private AtomicBoolean repeat;
    private CountDownLatch countDownLatch;
    private Producer<Long, String> linkProducer;
    private Producer<Long, Page> pageProducer;
    private Timer parseTimer;
    private Counter pagesCounter;

    public PageParserThread(ArrayBlockingQueue<String> queue, KafkaConfiguration kafkaConfiguration,
                            CountDownLatch parserLatch, CacheService cacheService, MetricConfiguration metrics) {
        this.queue = queue;
        this.kafkaConfiguration = kafkaConfiguration;
        countDownLatch = parserLatch;
        repeat = new AtomicBoolean(true);
        linkProducer = kafkaConfiguration.getLinkProducer();
        pageProducer = kafkaConfiguration.getPageProducer();
        parseTimer = metrics.getNewTimer(metrics.getProperty("crawler.page.parse.timer.name"));
        pagesCounter = metrics.getNewCounter(metrics.getProperty("crawler.pages.added.to.kafka.counter.name"));
    }

    // For Test
    public PageParserThread(Timer timer) {
        this.parseTimer = timer;
    }

    @Override
    public void run() {
        while (repeat.get()) {
            String uri = null;
            try {
                uri = queue.poll(100, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.error("interrupt exception in page parser", e);
            }
            if (uri == null) {
                continue;
            }
            logger.info("uri {} catches from queue", uri);
            Page elasticPage = null;
            Page hBasePage = null;
            try {
                PagePair parse = parse(uri);
                hBasePage = parse.getHBasePage();
                elasticPage = parse.getElasticPage();

                if (elasticPage == null || hBasePage == null) {
                    continue;
                }

                if (!elasticPage.isValid() || !hBasePage.isValid()) {
                    continue;
                }

                ProducerRecord<Long, Page> hBaseRecord = new ProducerRecord<>(kafkaConfiguration.getHBasePageTopicName(),
                        hBasePage);
                ProducerRecord<Long, Page> elasticRecord = new ProducerRecord<>(kafkaConfiguration.getElasticPageTopicName(),
                        elasticPage);
                pageProducer.send(hBaseRecord);
                pageProducer.send(elasticRecord);
                pagesCounter.inc();
                logger.info("page added to kafka");
                if (App.produceLink)
                    addLinksToKafka(hBasePage);
            } catch (Exception e) {
                logger.error("1 parser thread was going to interrupt", e);
            }

        }
        logger.info("before producers countDown latch");
        countDownLatch.countDown();
        logger.info("after producers countDown latch. countDown latch : {}", countDownLatch.getCount());
        try {
            pageProducer.close();
            linkProducer.close();
        } catch (Exception e) {
            logger.error("error in closing producer", e);
        }
    }

    private void addLinksToKafka(Page page) {
        for (HtmlTag htmlTag : page.getLinks()) {
            String link = htmlTag.getProps().get("href").trim();
            if (isValidUri(link)) {
                ProducerRecord<Long, String> record = new ProducerRecord<>(kafkaConfiguration.getLinkTopicName(), link);
                linkProducer.send(record);
            }
        }
    }

    /**
     * @return True if uri end with ".html" or ".htm" or ".asp[x]" or ".php" or the uri do not have any extension.
     */
    private boolean isValidUri(String link) {
        if (link.contains("softonic"))
            return false;
        try {
            while (link.endsWith("/")) {
                link = link.substring(0, link.length() - 1);
            }
            if (link.endsWith(".html") || link.endsWith(".htm") || link.endsWith(".php") || link.endsWith(".asp")
                    || link.endsWith(".aspx") || !link.substring(link.lastIndexOf('/') + 1).contains(".")) {
                return true;
            }
        } catch (IndexOutOfBoundsException e) {
            logger.info("invalid uri : {}", link);
            return false;
        }
        return false;
    }

    PagePair parse(String url) { // TODO refactor this function
        logger.info("start parsing...");
        Timer.Context context = parseTimer.time();
        Document document;
        Page elasticPage = new Page();
        Page hbasePage = new Page();
        elasticPage.setUrl(url);
        hbasePage.setUrl(url);
        try {
            Connection connect = Jsoup.connect(url);
            connect.timeout(2000);
            document = connect.get();
        } catch (Exception e) { //
            logger.error("exception in connection to url. empty page instance will return");
            return new PagePair(hbasePage, elasticPage);
        }
        for (Element element : document.getAllElements()) {
            Set<String> h3to6Tags = new HashSet<>(Arrays.asList("h3", "h4", "h5", "h6"));
            Set<String> plainTextTags = new HashSet<>(Arrays.asList("p", "span", "pre", "td", "li"));
            String text = element.text();
            if (text == null)
                text = "";
            if (h3to6Tags.contains(element.tagName().toLowerCase()))
                elasticPage.getH3to6List().add(new HtmlTag(element.tagName(), text));
            else if (plainTextTags.contains(element.tagName().toLowerCase()))
                elasticPage.getPlainTextList().add(new HtmlTag(element.tagName(), text));
            else if (element.tagName().equalsIgnoreCase("h1"))
                elasticPage.getH1List().add(new HtmlTag("h1", text));
            else if (element.tagName().equalsIgnoreCase("h2"))
                elasticPage.getH2List().add(new HtmlTag("h2", text));
            else if (element.tagName().equalsIgnoreCase("title"))
                elasticPage.setTitle(text);
            else if (element.tagName().equalsIgnoreCase("a")) {
                String href = element.attr("abs:href");
                if (href == null)
                    href = "";
                HtmlTag linkTag = new HtmlTag("a", text);
                linkTag.getProps().put("href", href);
                hbasePage.getLinks().add(linkTag);
            } else if (element.tagName().equalsIgnoreCase("meta")) {
                String name = element.attr("name");
                if (name == null)
                    name = "";
                String content = element.attr("content");
                if (content == null)
                    content = "";
                HtmlTag metaTag = new HtmlTag("meta");
                metaTag.getProps().put("name", name);
                metaTag.getProps().put("content", content);
                elasticPage.getMetadata().add(metaTag);
            }
        }
        context.stop();
        logger.info("parsing page done.");
        elasticPage.setValid(true);
        hbasePage.setValid(true);
        return new PagePair(hbasePage, elasticPage);
    }

    @Override
    public void interrupt() {
        logger.info("before setting producer thread to false");
        repeat.set(false);
        logger.info("after setting producer thread to false, repeat {}", repeat.get());
    }

    static class PagePair {
        private Page hBasePage;
        private Page elasticPage;

        PagePair(Page hBasePage, Page elasticPage) {
            this.hBasePage = hBasePage;
            this.elasticPage = elasticPage;
        }

        Page getHBasePage() {
            return hBasePage;
        }

        Page getElasticPage() {
            return elasticPage;
        }
    }
}
