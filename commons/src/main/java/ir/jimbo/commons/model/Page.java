package ir.jimbo.commons.model;

import java.util.ArrayList;
import java.util.List;

public class Page {
    private String url;
    private String title;
    private List<HtmlTag> metadata;
    private List<HtmlTag> links;
    private List<HtmlTag> h1List;
    private List<HtmlTag> h2List;
    private List<HtmlTag> h3to6List;
    private List<HtmlTag> plainTextList; //<p>, <pre> and <span> tags

    public Page() {
        this.url = "";
        this.title = "";
        this.metadata = new ArrayList<>();
        this.links = new ArrayList<>();
        this.h1List = new ArrayList<>();
        this.h2List = new ArrayList<>();
        this.h3to6List = new ArrayList<>();
        this.plainTextList = new ArrayList<>();
    }

    public Page(String url, String title) {
        this.url = url;
        this.title = title;
        this.metadata = new ArrayList<>();
        this.links = new ArrayList<>();
        this.h1List = new ArrayList<>();
        this.h2List = new ArrayList<>();
        this.h3to6List = new ArrayList<>();
        this.plainTextList = new ArrayList<>();
    }

    public Page(String url, String title, List<HtmlTag> metadata, List<HtmlTag> links, List<HtmlTag> h1List, List<HtmlTag> h2List, List<HtmlTag> h3to6List, List<HtmlTag> plainTextList) {
        this.url = url;
        this.title = title;
        this.metadata = metadata;
        this.links = links;
        this.h1List = h1List;
        this.h2List = h2List;
        this.h3to6List = h3to6List;
        this.plainTextList = plainTextList;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setMetadata(List<HtmlTag> metadata) {
        this.metadata = metadata;
    }

    public void setLinks(List<HtmlTag> links) {
        this.links = links;
    }

    public void setH1List(List<HtmlTag> h1List) {
        this.h1List = h1List;
    }

    public void setH2List(List<HtmlTag> h2List) {
        this.h2List = h2List;
    }

    public void setH3to6List(List<HtmlTag> h3to6List) {
        this.h3to6List = h3to6List;
    }

    public void setPlainTextList(List<HtmlTag> plainTextList) {
        this.plainTextList = plainTextList;
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public List<HtmlTag> getMetadata() {
        return metadata;
    }

    public List<HtmlTag> getLinks() {
        return links;
    }

    public List<HtmlTag> getH1List() {
        return h1List;
    }

    public List<HtmlTag> getH2List() {
        return h2List;
    }

    public List<HtmlTag> getH3to6List() {
        return h3to6List;
    }

    public List<HtmlTag> getPlainTextList() {
        return plainTextList;
    }
}
