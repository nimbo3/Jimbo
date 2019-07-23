package ir.jimbo.crawler.parse;

import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import ir.jimbo.crawler.PageParse;
import ir.jimbo.crawler.kafka.MyProducer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PageParseAndAddToKafka extends PageParse implements Runnable {

    private String url;
    MyProducer producer;
    String urlsTopicName;

    public PageParseAndAddToKafka(MyProducer producer, String urlsTopicName) {
        this.producer = producer;
        this.urlsTopicName = urlsTopicName;
    }

    public PageParseAndAddToKafka(String url) {
        this.url = url;
    }

    public PageParseAndAddToKafka() {
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
            Page page = parse();
            producer.addPageToKafka(urlsTopicName, page);
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
                page.getH3to6List().add(new HtmlTag(element.tagName(), element.text()));
            else if (plainTextTags.contains(element.tagName().toLowerCase()))
                page.getPlainTextList().add(new HtmlTag(element.tagName(), element.text()));
            else if (element.tagName().equalsIgnoreCase("h1"))
                page.getH1List().add(new HtmlTag("h1", element.text()));
            else if (element.tagName().equalsIgnoreCase("h2"))
                page.getH2List().add(new HtmlTag("h2", element.text()));
            else if (element.tagName().equalsIgnoreCase("title"))
                page.setTitle(element.text());
            else if (element.tagName().equalsIgnoreCase("a")) {
                String href = element.attr("abs:href");
                if (href == null)
                    href = "";
                HtmlTag linkTag = new HtmlTag("a", element.text());
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
                metaTag.getProps().put("name", element.attr("name"));
                metaTag.getProps().put("content", element.attr("content"));
                page.getMetadata().add(metaTag);
            }
        }

        return page;
    }
}
