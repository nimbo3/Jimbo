package ir.jimbo.web.graph.model;

public class GraphEdge {

    private String src;
    private String dst;
    private String incomingUrl;
    private String anchor;

    public GraphEdge() {

    }

    public String getIncomingUrl() {
        return incomingUrl;
    }

    public void setIncomingUrl(String incomingUrl) {
        this.incomingUrl = incomingUrl;
    }

    public String getAnchor() {
        return anchor;
    }

    public void setAnchor(String anchor) {
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
}
