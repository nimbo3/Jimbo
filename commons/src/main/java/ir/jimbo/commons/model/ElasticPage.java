package ir.jimbo.commons.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ToString
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
    private double pageRank;
    private double rank;
    private double pageRank;
    private String category;
    private String suggest;
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
        this.pageRank = 1;
        this.category = "";
        this.suggest = "";
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
        this.pageRank = 1;
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
        String h1 = h1List.stream().reduce("", (s1, s2) -> s1 + " " + s2);
        suggest = h1 + " " + title;
    }
}
