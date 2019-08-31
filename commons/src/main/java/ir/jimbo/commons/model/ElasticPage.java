package ir.jimbo.commons.model;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

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
    private String lang;
    private String id;
    private double rank;
    private String category;
    private int numberOfReferences;
    private List<String> topAnchors;

    public ElasticPage() {
        this.url = "";
        this.title = "";
        this.metaTags = new ArrayList<>();
        this.h1List = new ArrayList<>();
        this.h2List = new ArrayList<>();
        this.h3to6List = new ArrayList<>();
        this.text = "";
        this.lang = "";
        this.id = "";
        this.rank = 1;
        this.category = "";
    }

    // Map page to ElasticPage
    public ElasticPage(Page page) {
        this.url = page.getUrl();
        this.title = page.getTitle();
        this.metaTags = new ArrayList<>();
        this.h1List = new ArrayList<>();
        this.h2List = new ArrayList<>();
        this.h3to6List = new ArrayList<>();
        this.text = "";
        this.lang = "";
        this.id = "";
        this.rank = 1;
        this.category = "";

        for (HtmlTag meta : page.getMetadata()) {
            String metaString = meta.getProps().get("name") + ":: " + meta.getProps().get("content");
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
            if (htmlTag.getContent() != null && !htmlTag.getContent().trim().equals(""))
                stringBuilder.append(" ");
        }
        text = stringBuilder.toString();
    }

    @Override
    public String toString() {
        return "ElasticPage{" +
                "url='" + url + '\'' +
                ", title='" + title + '\'' +
                ", metaTags=" + metaTags +
                ", h1List=" + h1List +
                ", lang='" + lang + '\'' +
                '}';
    }
}
