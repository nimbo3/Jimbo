package ir.jimbo.web.graph;

import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.web.graph.config.AppConfiguration;
import ir.jimbo.web.graph.config.ElasticSearchConfiguration;
import ir.jimbo.web.graph.config.HBaseConfiguration;
import ir.jimbo.web.graph.manager.ElasticSearchService;
import ir.jimbo.web.graph.manager.HTableManager;
import ir.jimbo.web.graph.model.GraphEdge;
import ir.jimbo.web.graph.model.GraphVertex;
import ir.jimbo.web.graph.model.VerticesAndEdges;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
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
    private List<GraphVertex> graphVertices; // nodes

    public WebGraph(AppConfiguration appConfiguration) throws IOException, NoSuchAlgorithmException {
        elasticSearchService = new ElasticSearchService(ElasticSearchConfiguration.getInstance());
        this.appConfiguration = appConfiguration;
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration("hbase");
        hTableManager = new HTableManager(hBaseConfiguration.getTableName(), hBaseConfiguration.getColumnName());
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
        startSparkJobs();
        System.err.println("---------------------------------------- spark Job Done ----------------------------------------");
        createOutput();
    }

    public void createOutput() {
        VerticesAndEdges verticesAndEdges = new VerticesAndEdges();
        verticesAndEdges.setEdges(graphEdges);
        verticesAndEdges.setVertices(graphVertices);
        System.out.println(verticesAndEdges);
    }

    /**
     * getNoVersionMap of result return a navigableMap that have Column family name map to another navigable map
     * that contains qualifiers map to values
     *
     * @param elasticPages
     * @throws IOException
     */
    private void createVerticesAndEdges(List<ElasticPage> elasticPages) throws IOException {
        HashUtil hashUtil = new HashUtil();
        String url;
        MultiGetRequestBuilder multiGetRequestBuilder = elasticSearchService.getMultiGetRequestBuilder();
        ElasticSearchConfiguration elasticConfigs = ElasticSearchConfiguration.getInstance();
        List<Get> gets = new ArrayList<>();
        for (ElasticPage elasticPage : elasticPages) {
            url = elasticPage.getUrl();
            gets.add(hTableManager.getGet(url));
        }
        Result[] rows = hTableManager.getBulk(gets);
        for (Result row : rows) {
            if (row != null) {
                row.getNoVersionMap().forEach((a, b) -> b.forEach((qualifier, value) -> {
                    // Qualifier is hash of src url and value is its anchor
                    multiGetRequestBuilder.add(elasticConfigs.getIndexName(), "_doc"
                            , Bytes.toHex(qualifier).substring(32));
                }));
            }
        }
        List<ElasticPage> documents = elasticSearchService.getDocuments(multiGetRequestBuilder);

        for (ElasticPage elasticPage : elasticPages) {
            url = elasticPage.getUrl();
            graphVertices.add(new GraphVertex(url, 1, 2));

        }

        for (Result row : rows) {
            if (row != null) {
                row.getNoVersionMap().forEach((a, b) -> b.forEach((qualifier, value) -> {
                    for (ElasticPage elasticPage : documents) {
                        if (Bytes.toHex(qualifier).substring(32).equals(hashUtil.getMd5(elasticPage.getUrl()))) {
                            graphVertices.add(new GraphVertex(elasticPage.getUrl(), 0.5, 1));
                            graphEdges.add(new GraphEdge(elasticPage.getUrl(), , Bytes.toString(value)));
                            documents.remove(elasticPage);
                            break;
                        }
                    }
                }));
            }
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

        Dataset<Row> verticesDataFrame = spark.createDataFrame(graphVertices, GraphVertex.class);
        Dataset<Row> edgesDataFrame = spark.createDataFrame(graphEdges, GraphEdge.class);

        graphVertices.clear();

        GraphFrame graphFrame = new GraphFrame(verticesDataFrame, edgesDataFrame);

        graphFrame.stronglyConnectedComponents()
                .maxIter(10)
                .run()
                .toJavaRDD()
                .collect()
                .forEach(a -> graphVertices.add(new GraphVertex(a.get(1).toString()
                        , Double.parseDouble(a.get(2).toString()), Double.parseDouble(a.get(3).toString()))));

        spark.close();
    }

    public void setGraphEdges(List<GraphEdge> graphEdges) {
        this.graphEdges = graphEdges;
    }

    public void setGraphVertices(List<GraphVertex> graphVertices) {
        this.graphVertices = graphVertices;
    }
}
