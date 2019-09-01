package ir.jimbo.web.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.model.RankObject;
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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
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
        deleteBadEdges();
        logger.info("number of vertices : {}", graphVertices.size());
        logger.info("number of edges : {}", graphEdges.size());
        startSparkJobs();
        System.err.println("---------------------------------------- spark Job Done ----------------------------------------");
        createOutput();
    }

    public void createOutput() {
        logger.info("fixing ranks ...");
        fixRanks();
        logger.info("ranks fixed");
        VerticesAndEdges verticesAndEdges = new VerticesAndEdges();
        verticesAndEdges.setEdges(graphEdges);
        verticesAndEdges.setVertices(graphVertices);
        try {
            hTableManager.close();
        } catch (IOException e) {
            logger.error("exception in closing hBase manager", e);
        }
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try (FileWriter fileWriter = new FileWriter("jsonFormat.txt")) {
            String json = ow.writeValueAsString(verticesAndEdges);
            fileWriter.write(json);
            fileWriter.flush();
        } catch (IOException e) {
            logger.error(e);
        }
    }

    private void fixRanks() {
        SearchResponse topIds = elasticSearchService.getSearchResponse();
        List<RankObject> rankObjects = new ArrayList<>();
        ObjectMapper reader = new ObjectMapper();
        HashUtil hashUtil = new HashUtil();
        for (SearchHit hit : topIds.getHits()) {
            try {
                rankObjects.add(reader.readValue(hit.getSourceAsString(), RankObject.class));
            } catch (IOException e) {
                logger.error("cannot create rank object", e);
            }
        }
        for (GraphVertex graphVertex : graphVertices) {
            int size = rankObjects.size();
            for (int i = 0; i < size; i++) {
                if (hashUtil.getMd5(graphVertex.getId()).equals(rankObjects.get(i).getId())) {
                    graphVertex.setPagerank(rankObjects.get(i).getRank());
                    rankObjects.remove(i);
                    break;
                }
            }
        }
    }

    private void deleteBadEdges() {
        logger.info("deleting bad edges");
        Set<String> ids = new HashSet<>();
        for (GraphVertex graphVertex : graphVertices) {
            ids.add(graphVertex.getId());
        }
        int size = graphEdges.size();
        for (int i = 0; i < size; i++) {
            if (! ids.contains(graphEdges.get(i).getSrc())) {
                graphEdges.remove(i);
                size --;
                i --;
            }
        }
    }

    /**
     * getNoVersionMap of result return a navigableMap that have Column family name map to another navigable map
     * that contains qualifiers map to values
     *
     * @param elasticPages source elastic pages
     */
    private void createVerticesAndEdges(List<ElasticPage> elasticPages) {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger attempt = new AtomicInteger();
        for (ElasticPage elasticPage : elasticPages) {
            Result record = null;
            try {
                record = hTableManager.getRecord(elasticPage.getUrl());
            } catch (IOException e) {
                logger.error("line 113", e);
            }
            if (record == null)
                continue;
            graphVertices.add(new GraphVertex(elasticPage.getUrl(), 1, 1));
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = record.getNoVersionMap();
            if (noVersionMap == null) {
                continue;
            }
            noVersionMap.forEach((a, b) -> {
                if (b == null) {
                    return;
                }
                b.forEach((qualifier, value) -> {
                    try {
                        String elasticId = Bytes.toHex(qualifier).substring(32);
                        ElasticPage elasticDocument = elasticSearchService.getDocument(elasticId);
                        if (elasticDocument != null) {
//                            graphVertices.add(new GraphVertex(elasticDocument.getUrl(), 0.5, 1));
                            graphEdges.add(new GraphEdge(elasticDocument.getUrl(), elasticPage.getUrl(), Bytes.toString(value)));
                            if (attempt.getAndIncrement() > 999) {
                                attempt.set(0);
                                logger.info("1000 edge added.");
                            }
                        }
                    } catch (Exception e) {
                        logger.error("line 135, get elastic documents");
                        counter.getAndIncrement();
                    }
                });
            });
        }
        logger.info("-_-_-_-_-_-_-_-_-_-_-_-_-_- {} bad mapping -_-_-_-_-_-_-_-_-_-_-_-_-_-", counter.get());
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
