package ir.jimbo.crawler.parse;

import ir.jimbo.commons.model.Page;
import ir.jimbo.commons.model.TitleAndLink;
import ir.jimbo.crawler.PageParse;
import ir.jimbo.crawler.exceptions.NoDomainFoundException;
import ir.jimbo.crawler.kafka.MyProducer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageParseAndAddToKafka extends PageParse implements Runnable {

    private String url;
    private Pattern domainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    MyProducer producer;
    String urlsTopicName;
    String pagesTopicName;

    public PageParseAndAddToKafka(MyProducer producer, String urlsTopicName, String pagesTopicName) {
        this.producer = producer;
        this.urlsTopicName = urlsTopicName;
        this.pagesTopicName = pagesTopicName;
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
            Page page = parse();
            producer.addPageToKafka(pagesTopicName, page);
            redis.addDomainInDb(getDomain(url));
            for (Map.Entry<String, String> values : page.getLinks().entrySet()) {
                if (checkValidUrl(values.getValue())) {
                    producer.addLinkToKafka(urlsTopicName, new TitleAndLink(values.getKey(), values.getValue()));
                }
            }
        }
    }

    public Page parse() {
        Document document = null;
        Page page = new Page();
        try {
            document = Jsoup.connect(url).get();
        } catch (IOException e) {
            // TODO: Log
            e.printStackTrace();
            return page;
        }

        for (Element element : document.getAllElements()) {
            Set<String> h3to6Tags = new HashSet<>(Arrays.asList("h3", "h4", "h5", "h6"));
            Set<String> plainTextTags = new HashSet<>(Arrays.asList("p", "span", "pre"));
            String text = element.text();
            if (text == null)
                text = "";
            if (h3to6Tags.contains(element.tagName().toLowerCase()))
                page.getH3to6List().add(text);
            else if (plainTextTags.contains(element.tagName().toLowerCase()))
                page.getPlainTextList().add(text);
            else if (element.tagName().equalsIgnoreCase("h1"))
                page.getH1List().add(text);
            else if (element.tagName().equalsIgnoreCase("h2"))
                page.getH2List().add(text);
            else if (element.tagName().equalsIgnoreCase("title"))
                page.setTitle(element.text());
            else if (element.tagName().equalsIgnoreCase("a")) {
                String href = element.attr("href");
                if (href == null)
                    href = "";
                page.getLinks().put(text, href);
            } else if (element.tagName().equalsIgnoreCase("meta")) {
                String name = element.attr("name");
                if (name == null)
                    name = "";
                String content = element.attr("content");
                if (content == null)
                    content = "";
                page.getMetadata().put(name, content);
            }
        }

        return page;
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
