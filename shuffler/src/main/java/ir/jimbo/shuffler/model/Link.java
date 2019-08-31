package ir.jimbo.shuffler.model;

public class Link {

    private String domain;
    private String url;

    public Link(String domain, String url) {
        this.domain = domain;
        this.url = url;
    }

    public String getDomain() {
        return domain;
    }

    public String getUrl() {
        return url;
    }
}
