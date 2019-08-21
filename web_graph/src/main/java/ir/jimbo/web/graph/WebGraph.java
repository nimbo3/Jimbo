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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class WebGraph {

    private final Logger LOGGER = LogManager.getLogger(WebGraph.class);
    private ElasticSearchService elasticSearchService;
    private AppConfiguration appConfiguration;
    private HTableManager hTableManager;
    private Set<GraphEdge> graphEdges;    // yalha
    private Set<GraphVertice> graphVertices; // nodes
    private HashUtil hashUtil;

    WebGraph() throws IOException, NoSuchAlgorithmException {
        elasticSearchService = new ElasticSearchService(ElasticSearchConfiguration.getInstance());
        appConfiguration = new AppConfiguration();
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration("hbase");
        hTableManager = new HTableManager(hBaseConfiguration.getTableName(), hBaseConfiguration.getColumnName());
        hashUtil = new HashUtil();
    }

    public void start() throws IOException {
        graphEdges = new HashSet<>();
        graphVertices = new HashSet<>();
        LOGGER.info("reading pages from elastic");
//        List<ElasticPage> elasticPages = getFromElastic();
//        LOGGER.info("{} pages readed from elastic", elasticPages.size());
        LOGGER.info("start finding links on hbase and create vertices and edges lists...");
//        elasticPages.clear();
//        ElasticPage elasticPage = new ElasticPage();
//        elasticPage.setUrl("https://hu.wikipedia.org/wiki/Saint-Maurice_(Val-de-Marne)");
//        elasticPages.add(elasticPage);
//        createVerticesAndEdges(elasticPages);
        ElasticPage document = elasticSearchService.getDocument("055313870c185681932683aba17b9499");
        System.err.println("url" + document.getUrl());
        System.err.println(document);
        LOGGER.info("creating vertices lists");
    }

    /**
     * getNoVersionMap of result return a navigableMap that have Column family name map to another navigable map
     * that contains qualifiers map to values
     *
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
            LOGGER.info("creating a vertice by url with high rank, url : {}", elasticPage.getUrl());
            LOGGER.info("hash util {}", hashUtil);
            LOGGER.info("get md5 : {}", hashUtil.getMd5(elasticPage.getUrl()));
            GraphVertice vertice = new GraphVertice(hashUtil.getMd5(elasticPage.getUrl()), elasticPage.getUrl(), 1.0);
            LOGGER.info("vertice : {}", vertice);
            LOGGER.info("graph vertices : {}", graphVertices);
            graphVertices.add(vertice);
            LOGGER.info("creating some vertice and edges (vertice for incoming links that may not have high rank)");
            record.getNoVersionMap().forEach((a, b) -> b.forEach((qualifier, value) -> {
                // Qualifier is hash of src url and value is its anchor
                LOGGER.info("qualifier readed from hbase {}", Bytes.toHex(qualifier));
                ElasticPage document = null;
                try {
                    document = elasticSearchService.getDocument(Bytes.toHex(qualifier));
                } catch (IOException e) {
                    LOGGER.error("exception in getting document from elastic", e);
                }
                if (document == null) {
                    LOGGER.warn("page in not in elastic.qualifier : {}", Bytes.toHex(qualifier));
                } else {
                    LOGGER.info("a follower url : {}", document.getUrl());
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
}
