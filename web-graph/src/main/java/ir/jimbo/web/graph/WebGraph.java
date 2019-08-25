package ir.jimbo.web.graph;

import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.web.graph.config.AppConfiguration;
import ir.jimbo.web.graph.config.ElasticSearchConfiguration;
import ir.jimbo.web.graph.config.HBaseConfiguration;
import ir.jimbo.web.graph.manager.ElasticSearchService;
import ir.jimbo.web.graph.manager.HTableManager;
import ir.jimbo.web.graph.model.GraphEdge;
import ir.jimbo.web.graph.model.GraphVertice;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class WebGraph {

    private final Logger LOGGER = LogManager.getLogger(WebGraph.class);
    private ElasticSearchService elasticSearchService;
    private AppConfiguration appConfiguration;
    private HTableManager hTableManager;
    private List<GraphEdge> graphEdges;    // yalha
    private List<GraphVertice> graphVertices; // nodes
    private HashUtil hashUtil;

    public WebGraph(AppConfiguration appConfiguration) throws IOException, NoSuchAlgorithmException {
        elasticSearchService = new ElasticSearchService(ElasticSearchConfiguration.getInstance());
        this.appConfiguration = appConfiguration;
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration("hbase");
        hTableManager = new HTableManager(hBaseConfiguration.getTableName(), hBaseConfiguration.getColumnName());
        hashUtil = new HashUtil();
        graphEdges = new ArrayList<>();
        graphVertices = new ArrayList<>();
    }

    public WebGraph() {
        graphEdges = new ArrayList<>();
        graphVertices = new ArrayList<>();
    }

    public void start() throws IOException {
        LOGGER.info("reading pages from elastic");
        List<ElasticPage> elasticPages = getFromElastic();
        LOGGER.info("{} pages readed from elastic", elasticPages.size());
        LOGGER.info("start finding links on hbase and create vertices and edges lists...");
        createVerticesAndEdges(elasticPages);
        LOGGER.info("vertices and edges created");
        System.out.println(graphVertices);
        System.out.println(graphEdges);
        startSparkJobs();
    }

    /**
     * getNoVersionMap of result return a navigableMap that have Column family name map to another navigable map
     * that contains qualifiers map to values
     * @param elasticPages
     * @return
     * @throws IOException
     */
    private void createVerticesAndEdges(List<ElasticPage> elasticPages) throws IOException {
        String url;
        for (ElasticPage elasticPage : elasticPages) {
            url = elasticPage.getUrl();
            LOGGER.info("start getting {} from hbase", url);
            Result record = hTableManager.getRecord(url);
            if (record.isEmpty()) {
                LOGGER.warn("url {} is not in hbase", url);
                continue;
            }
            GraphVertice vertex = new GraphVertice(elasticPage.getUrl(), 1.0);
            graphVertices.add(vertex);
            LOGGER.info("creating some vertex and edges (vertex for incoming links that may not have high rank)");
            record.getNoVersionMap().forEach((a, b) -> b.forEach((qualifier, value) -> {
                // Qualifier is hash of src url and value is its anchor
                ElasticPage document = null;
                try {
                    document = elasticSearchService.getDocument(Bytes.toHex(qualifier).substring(32));
                } catch (IOException e) {
                    LOGGER.error("exception in getting document from elastic", e);
                }
                if (document == null) {
                    LOGGER.warn("page in not in elastic.qualifier : {}", Bytes.toHex(qualifier).substring(32));
                } else {
                    LOGGER.info("a follower url : {}", document.getUrl());
                    graphVertices.add(new GraphVertice(document.getUrl(), 0.5));
                    graphEdges.add(new GraphEdge(document.getUrl(), elasticPage.getUrl(), Bytes.toString(value)));
                }
            }));
        }
    }

    private List<ElasticPage> getFromElastic() {
        boolean repeat = true;
        List<ElasticPage> elasticPages = new ArrayList<>();
        while (repeat) {
            elasticPages.addAll(elasticSearchService.getSourcePages());
            LOGGER.info("{} pages readed to now from elastic", elasticPages.size());
            if (elasticPages.size() >= appConfiguration.getGraphNodeNumber())
                repeat = false;
        }
        return elasticPages;
    }

    public void startSparkJobs() {
        SparkSession spark = SparkSession.builder()
                .appName("web_graph")
                .master("local")
                .getOrCreate();

        Dataset<Row> verticesDataFrame = spark.createDataFrame(graphVertices, GraphVertice.class);
        Dataset<Row> edgesDataFrame = spark.createDataFrame(graphEdges, GraphEdge.class);

        GraphFrame graphFrame = new GraphFrame(verticesDataFrame, edgesDataFrame);
        graphFrame.vertices().show();
        graphFrame.edges().show();
        graphFrame
                .stronglyConnectedComponents()
                .maxIter(10)
                .run()
                .show();

        spark.close();
    }

    public void setGraphEdges(List<GraphEdge> graphEdges) {
        this.graphEdges = graphEdges;
    }

    public void setGraphVertices(List<GraphVertice> graphVertices) {
        this.graphVertices = graphVertices;
    }
}
