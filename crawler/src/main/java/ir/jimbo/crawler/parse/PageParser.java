package ir.jimbo.crawler.parse;

import ir.jimbo.commons.model.Page;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PageParser {

    private String url;

    PageParser(String url) {
        this.url = url;
    }

    public Page parse() {
        Document document = Jsoup.parse(url);
        Page page = new Page();

        for (Element element : document.getAllElements()) {
            Set<String> h3to6Tags = new HashSet<>(Arrays.asList("h3", "h4", "h5", "h6"));
            Set<String> plainTextTags = new HashSet<>(Arrays.asList("p", "span", "pre"));
            String text = element.text();
            if(text == null)
                text = "";
            if (h3to6Tags.contains(element.tagName()))
                page.getH3to6List().add(text);
            if (plainTextTags.contains(element.tagName()))
                page.getPlainTextList().add(text);
            if (element.tagName().equals("h1"))
                page.getH1List().add(text);
            if (element.tagName().equals("h2"))
                page.getH2List().add(text);
            if (element.tagName().equals("a")) {
                String href = element.attr("href");
                if (href == null)
                    href = "";
                page.getLinks().put(text, href);
            }
            if (element.tagName().equals("meta")) {
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
}
