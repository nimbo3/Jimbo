package ir.jimbo.crawler.parse;

import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PageParser {

    private Logger logger = LogManager.getLogger(this.getClass());
    private String url;

    public PageParser(String url) {
        this.url = url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public Page parse() {
        System.out.println("start parsing");
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
