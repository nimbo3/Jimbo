package ir.jimbo.web.graph.model;

public class GraphVertice {

    private String id;
    private String url;
    private double pagerank;

    public GraphVertice() {
    }

    public GraphVertice(String id, String url, double pagerank) {
        this.id = id;
        this.url = url;
        this.pagerank = pagerank;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public double getPagerank() {
        return pagerank;
    }

    public void setPagerank(double pagerank) {
        this.pagerank = pagerank;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "GraphVertice{" +
                "id='" + id + '\'' +
                ", url='" + url + '\'' +
                ", rank=" + pagerank +
                '}';
    }
}
