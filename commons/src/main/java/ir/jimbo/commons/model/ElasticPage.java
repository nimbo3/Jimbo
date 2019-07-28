package ir.jimbo.commons.model;

import lombok.Getter;
import lombok.Setter;
import java.util.ArrayList;
import java.util.List;

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

    public void setUrl(String url) {
        this.url = url;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setMetaTags(List<String> metaTags) {
        this.metaTags = metaTags;
    }

    public void setH1List(List<String> h1List) {
        this.h1List = h1List;
    }

    public void setH2List(List<String> h2List) {
        this.h2List = h2List;
    }

    public void setH3to6List(List<String> h3to6List) {
        this.h3to6List = h3to6List;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public List<String> getMetaTags() {
        return metaTags;
    }

    public List<String> getH1List() {
        return h1List;
    }

    public List<String> getH2List() {
        return h2List;
    }

    public List<String> getH3to6List() {
        return h3to6List;
    }

    public String getText() {
        return text;
    }
}
