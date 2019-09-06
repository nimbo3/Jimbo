package ir.jimbo.web.graph;

import ir.jimbo.web.graph.model.GraphEdge;
import ir.jimbo.web.graph.model.GraphVertex;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class WebGraphTest {

    private static WebGraph webGraph;

    @BeforeClass
    public static void init() {
        webGraph = new WebGraph();
    }

    @Before
    public void initGraphEdgesSets() {
        ArrayList<GraphVertex> graphVertices = new ArrayList<>();
        graphVertices.add(new GraphVertex("www.a.com", 1.0, 1));
        graphVertices.add(new GraphVertex("www.b.com", 0.8, 1));
        graphVertices.add(new GraphVertex("www.c.com", 0.9, 1));
        graphVertices.add(new GraphVertex("www.d.com", 0.8, 1));
        graphVertices.add(new GraphVertex("www.e.com", 1.0, 1));
        graphVertices.add(new GraphVertex("www.f.com", 1.1, 1));
        graphVertices.add(new GraphVertex("www.g.com", 1.2, 1));
        graphVertices.add(new GraphVertex("www.h.com", 1.3, 1));
        graphVertices.add(new GraphVertex("www.l.com", 1.4, 1));
        graphVertices.add(new GraphVertex("www.i.com", 1.05, 1));
        webGraph.setGraphVertices(graphVertices);
    }

    @Before
    public void initGraphVerticesSet() {
        ArrayList<GraphEdge> graphEdges = new ArrayList<>();
        graphEdges.add(new GraphEdge("www.b.com", "www.c.com", "a"));
        graphEdges.add(new GraphEdge("www.b.com", "www.d.com", "b"));
        graphEdges.add(new GraphEdge("www.b.com", "www.e.com", "c"));
        graphEdges.add(new GraphEdge("www.b.com", "www.f.com", "d"));
        graphEdges.add(new GraphEdge("www.b.com", "www.g.com", "e"));
        graphEdges.add(new GraphEdge("www.b.com", "www.h.com", "h"));
        graphEdges.add(new GraphEdge("www.c.com", "www.b.com", "i"));
        graphEdges.add(new GraphEdge("www.c.com", "www.i.com", "df"));
        graphEdges.add(new GraphEdge("www.d.com", "www.i.com", "dd"));
        graphEdges.add(new GraphEdge("www.d.com", "www.b.com", "sdv"));
        graphEdges.add(new GraphEdge("www.d.com", "www.f.com", "sdv"));
        graphEdges.add(new GraphEdge("www.e.com", "www.h.com", "asd"));
        graphEdges.add(new GraphEdge("www.f.com", "www.d.com", "fvsdzvc"));
        graphEdges.add(new GraphEdge("www.g.com", "www.g.com", "svd"));
        graphEdges.add(new GraphEdge("www.h.com", "www.c.com", "vd"));
        graphEdges.add(new GraphEdge("www.i.com", "www.i.com", "dsvs"));
        graphEdges.add(new GraphEdge("www.i.com", "www.l.com", "svd"));
        graphEdges.add(new GraphEdge("www.i.com", "www.h.com", "sf"));
        graphEdges.add(new GraphEdge("www.i.com", "www.g.com", "Sdgv"));
        graphEdges.add(new GraphEdge("www.i.com", "www.f.com", "Sdv"));
        webGraph.setGraphEdges(graphEdges);
    }

//    @Test
//    public void startSparkJobs() throws IOException {
//        webGraph.startSparkJobs();
//    }
}