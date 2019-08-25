package ir.jimbo.web.graph.model;

public class GraphVertice {

    private String id;
    private double pagerank;

    public GraphVertice() {
    }

    public GraphVertice(String id, double pagerank) {
        this.id = id;
        this.pagerank = pagerank;
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
                ", rank=" + pagerank +
                '}';
    }
}
