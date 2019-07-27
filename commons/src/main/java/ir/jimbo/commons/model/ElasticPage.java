package ir.jimbo.commons.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ElasticPage {
    private String url;
    private String title;
    private List<String> metaTags;
    private List<String> h1List;
    private List<String> h2List;
    private List<String> h3to6List;
    private String text;

    public ElasticPage() {
        this.url = "";
        this.title = "";
        this.h1List = new ArrayList<>();
        this.h2List = new ArrayList<>();
        this.h3to6List = new ArrayList<>();
        this.text = "";
        this.metaTags = new ArrayList<>();
    }

    // Map page to ElasticPage
    public ElasticPage(Page page) {
        this.url = page.getUrl();
        this.title = page.getTitle();
        this.h1List = new ArrayList<>();
        this.h2List = new ArrayList<>();
        this.h3to6List = new ArrayList<>();
        this.text = "";
        this.metaTags = new ArrayList<>();
        for (HtmlTag meta : page.getMetadata()) {
            String metaString = meta.getProps().get("name")+ ":: " + meta.getProps().get("content");
            metaTags.add(metaString);
        }
        for (HtmlTag htmlTag : page.getH1List()) {
            h1List.add(htmlTag.getContent());
        }
        for (HtmlTag htmlTag : page.getH2List()) {
            h2List.add(htmlTag.getContent());
        }
        for (HtmlTag htmlTag : page.getH3to6List()) {
            h3to6List.add(htmlTag.getContent());
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (HtmlTag htmlTag : page.getPlainTextList()) {
            stringBuilder.append(htmlTag.getContent());
        }
        text = stringBuilder.toString();
    }
}
