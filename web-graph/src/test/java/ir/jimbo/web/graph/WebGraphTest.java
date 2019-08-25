package ir.jimbo.web.graph;

import ir.jimbo.web.graph.WebGraph;
import ir.jimbo.web.graph.model.GraphEdge;
import ir.jimbo.web.graph.model.GraphVertice;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class WebGraphTest {

    private static WebGraph webGraph;

    @BeforeClass
    public static void init() throws IOException, NoSuchAlgorithmException {
        webGraph = new WebGraph();
    }

    @Before
    public void initGraphEdgesSets() {
        ArrayList<GraphVertice> graphVertices = new ArrayList<>();
        graphVertices.add(new GraphVertice("0", "www.a.com", 1.0));
        graphVertices.add(new GraphVertice("1", "www.b.com", 0.8));
        graphVertices.add(new GraphVertice("2", "www.c.com", 0.9));
        graphVertices.add(new GraphVertice("3", "www.d.com", 0.8));
        graphVertices.add(new GraphVertice("4", "www.e.com", 1.0));
        graphVertices.add(new GraphVertice("5", "www.f.com", 1.1));
        graphVertices.add(new GraphVertice("6", "www.g.com", 1.2));
        graphVertices.add(new GraphVertice("7", "www.h.com", 1.3));
        graphVertices.add(new GraphVertice("8", "www.l.com", 1.4));
        graphVertices.add(new GraphVertice("9", "www.i.com", 1.05));
        webGraph.setGraphVertices(graphVertices);
    }

    @Before
    public void initGraphVerticesSet() {
        ArrayList<GraphEdge> graphEdges = new ArrayList<>();
        graphEdges.add(new GraphEdge("1", "2"));
        graphEdges.add(new GraphEdge("1", "3"));
        graphEdges.add(new GraphEdge("1", "4"));
        graphEdges.add(new GraphEdge("1", "5"));
        graphEdges.add(new GraphEdge("1", "6"));
        graphEdges.add(new GraphEdge("1", "7"));
        graphEdges.add(new GraphEdge("2", "1"));
        graphEdges.add(new GraphEdge("2", "9"));
        graphEdges.add(new GraphEdge("3", "9"));
        graphEdges.add(new GraphEdge("3", "1"));
        graphEdges.add(new GraphEdge("3", "5"));
        graphEdges.add(new GraphEdge("4", "7"));
        graphEdges.add(new GraphEdge("5", "3"));
        graphEdges.add(new GraphEdge("6", "6"));
        graphEdges.add(new GraphEdge("7", "2"));
        graphEdges.add(new GraphEdge("9", "9"));
        graphEdges.add(new GraphEdge("9", "8"));
        graphEdges.add(new GraphEdge("9", "7"));
        graphEdges.add(new GraphEdge("9", "6"));
        graphEdges.add(new GraphEdge("9", "5"));
        webGraph.setGraphEdges(graphEdges);
    }

    @Test
    public void startSparkJobs() {
//        webGraph.startSparkJobs();
    }
}