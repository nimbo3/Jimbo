package ir.jimbo.web.graph.model;

public class GraphVertice {

    private String id;
    private String url;
    private double rank;

    public GraphVertice() {
    }

    public GraphVertice(String id, String url, double rank) {
        this.id = id;
        this.url = url;
        this.rank = rank;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
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
                ", rank=" + rank +
                '}';
    }
}
