package ir.jimbo.commons.model;

import java.util.ArrayList;
import java.util.List;

public class ElasticPage {
    private String url;
    private String title;
    private List<String> h1List;
    private List<String> h2List;
    private List<String> h3to6List;
    private StringBuilder text;

    // Map page to ElasticPage
    private ElasticPage(Page page) {
        this.url = page.getUrl();
        this.title = page.getTitle();
        this.h1List = new ArrayList<>();
        this.h2List = new ArrayList<>();
        this.h3to6List = new ArrayList<>();
        this.text = new StringBuilder();
        for (HtmlTag htmlTag : page.getH1List()) {
            h1List.add(htmlTag.getContent());
        }
        for (HtmlTag htmlTag : page.getH2List()) {
            h2List.add(htmlTag.getContent());
        }
        for (HtmlTag htmlTag : page.getH3to6List()) {
            h3to6List.add(htmlTag.getContent());
        }
        for (HtmlTag htmlTag : page.getPlainTextList()) {
            text.append(htmlTag.getContent());
        }
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
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

    public StringBuilder getText() {
        return text;
    }
}
