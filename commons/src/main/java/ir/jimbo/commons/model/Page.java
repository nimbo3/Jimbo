package ir.jimbo.commons.model;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
@Getter
@Setter
public class Page {
    private String url;
    private String title;
    private List<HtmlTag> metadata;
    private List<HtmlTag> links;
    private List<HtmlTag> h1List;
    private List<HtmlTag> h2List;
    private List<HtmlTag> h3to6List;
    private List<HtmlTag> plainTextList; //<p>, <pre> and <span> tags
    private boolean isValid;

    public Page() {
        this.url = "";
        this.title = "";
        this.metadata = new ArrayList<>();
        this.links = new ArrayList<>();
        this.h1List = new ArrayList<>();
        this.h2List = new ArrayList<>();
        this.h3to6List = new ArrayList<>();
        this.plainTextList = new ArrayList<>();
        this.isValid = false;
    }

    public Page(Page page) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Page page = (Page) o;
        return Objects.equals(url, page.url) &&
                Objects.equals(title, page.title) &&
                Objects.equals(metadata, page.metadata) &&
                Objects.equals(links, page.links) &&
                Objects.equals(h1List, page.h1List) &&
                Objects.equals(h2List, page.h2List) &&
                Objects.equals(h3to6List, page.h3to6List) &&
                Objects.equals(plainTextList, page.plainTextList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, title, metadata, links, h1List, h2List, h3to6List, plainTextList);
    }

    @Override
    public String toString() {
        return "Page{" +
                "url='" + url + '\'' +
                ", title='" + title + '\'' +
                ", metadata=" + metadata +
                ", links=" + links +
                ", h1List=" + h1List +
                ", h2List=" + h2List +
                ", h3to6List=" + h3to6List +
                ", plainTextList=" + plainTextList +
                ", isValid=" + isValid +
                '}';
    }
}
