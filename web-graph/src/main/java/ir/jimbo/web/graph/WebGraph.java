package ir.jimbo.web.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;

import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WebGraph {

    private final Logger logger = LogManager.getLogger(WebGraph.class);
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

    public void start() {
        logger.info("reading pages from elastic");
        List<ElasticPage> elasticPages = getFromElastic();
        logger.info("{} pages readed from elastic", elasticPages.size());
        logger.info("start finding links on hbase and create vertices and edges lists...");
        createVerticesAndEdges(elasticPages);
        logger.info("vertices and edges created");
        logger.info("number of vertices : {}", graphVertices.size());
        logger.info("number of edges : {}", graphEdges.size());
        startSparkJobs();
        System.err.println("---------------------------------------- spark Job Done ----------------------------------------");
        createOutput();
        logger.info("number of vertices : {}", graphVertices.size());
        logger.info("number of edges : {}", graphEdges.size());
    }

    public void createOutput() {
        VerticesAndEdges verticesAndEdges = new VerticesAndEdges();
        verticesAndEdges.setLinks(graphEdges);
        verticesAndEdges.setNodes(graphVertices);
        try {
            hTableManager.close();
        } catch (IOException e) {
            logger.error("exception in closing hBase manager", e);
        }
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try (FileWriter fileWriter = new FileWriter("graph.json")) {
            String json = ow.writeValueAsString(verticesAndEdges);
            fileWriter.write(json);
            fileWriter.flush();
        } catch (IOException e) {
            logger.error(e);
        }
    }

    private void createVerticesAndEdges(List<ElasticPage> elasticPages) {
        List<String> hashes = new ArrayList<>();
        logger.info("reading from hbase ...");
        AtomicInteger count = new AtomicInteger(0);
        HashUtil hashUtil = new HashUtil();
        for (ElasticPage elasticPage : elasticPages) {
            hashes.add(hashUtil.getMd5(elasticPage.getUrl()));
        }
        for (ElasticPage elasticPage : elasticPages) {
            graphVertices.add(new GraphVertex(elasticPage.getUrl(), elasticPage.getRank(), 1));
            Result record = null;
            try {
                record = hTableManager.getRecord(elasticPage.getUrl());
            } catch (IOException e) {
                logger.error(e);
            }
            if (record == null)
                continue;
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = record.getNoVersionMap();
            if (noVersionMap != null && !noVersionMap.isEmpty()) {
                Result finalRecord = record;
                noVersionMap.forEach((a, b) -> {
                    if (!Bytes.toString(a).equals("t") || b == null) {
                        return;
                    }
                    logger.info("walking on the {}th site", count.incrementAndGet());
                    b.forEach((qualifier, value) -> {
                        String urlHash = null;
                        try {
                            if (qualifier == null) {
                                return;
                            }
                            urlHash = Bytes.toHex(qualifier);
                            urlHash = urlHash.substring(32);
                        } catch (Exception e) {
                            urlHash = Bytes.toHex(qualifier);
                        }
                        for (String hash : hashes) {
                            if (hash.equals(urlHash)) {
                                logger.info("row key : {}", Bytes.toHex(finalRecord.getRow()).substring(32));
                                graphEdges.add(new GraphEdge(urlHash, Bytes.toHex(finalRecord.getRow()).substring(32)
                                        , Bytes.toString(value)));
                                break;
                            }
                        }
                    });
                });
            }
            noVersionMap = null;
        }
        logger.info("done walking");
        int counter = 0;
        for (GraphEdge graphEdge : graphEdges) {
            for (ElasticPage elasticPage : elasticPages) {
                if (hashUtil.getMd5(elasticPage.getUrl()).equals(graphEdge.getSrc())) {
                    graphEdge.setSrc(elasticPage.getUrl());
                    counter ++;
                }
                if (hashUtil.getMd5(elasticPage.getUrl()).equals(graphEdge.getDst())) {
                    graphEdge.setDst(elasticPage.getUrl());
                    counter ++;
                }
            }
        }
        logger.info("number of src and dst change : {}", counter);
        logger.info("done fixing edges");
    }

    private List<ElasticPage> getFromElastic() {
        boolean repeat = true;
        List<ElasticPage> elasticPages = new ArrayList<>();
        while (repeat) {
            elasticPages.addAll(elasticSearchService.getSourcePagesSorted());
            logger.info("{} pages readed to now from elastic", elasticPages.size());
            if (elasticPages.size() == appConfiguration.getGraphNodeNumber())
                repeat = false;
            else {
                logger.error("sorce pages readed from elastic number {}", elasticPages.size());
                System.exit(0);
            }
        }
        return elasticPages;
    }

    public void startSparkJobs() {
        SparkSession spark = SparkSession.builder()
                .appName("web_graph")
                .master("local")
                .getOrCreate();

        spark.sparkContext()
                .setLogLevel("ERROR");

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
