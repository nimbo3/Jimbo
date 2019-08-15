package module;


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

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
