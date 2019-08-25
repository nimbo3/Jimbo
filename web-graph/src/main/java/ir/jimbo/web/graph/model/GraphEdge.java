package ir.jimbo.web.graph.model;

public class GraphEdge {

    private String src;
    private String dst;
    private String anchor;

    public GraphEdge() {

    }

    public GraphEdge(String src, String dst, String anchor) {
        this.src = src;
        this.dst = dst;
        this.anchor = anchor;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public String getAnchor() {
        return anchor;
    }

    public void setAnchor(String anchor) {
        this.anchor = anchor;
    }

    @Override
    public String toString() {
        return "GraphEdge{" +
                "src='" + src + '\'' +
                ", dst='" + dst + '\'' +
                ", anchor='" + anchor + '\'' +
                '}';
    }
}
