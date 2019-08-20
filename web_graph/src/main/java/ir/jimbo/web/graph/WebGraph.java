package ir.jimbo.web.graph;

import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.web.graph.config.AppConfiguration;
import ir.jimbo.web.graph.config.ElasticSearchConfiguration;
import ir.jimbo.web.graph.manager.ElasticSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WebGraph {

    private final Logger LOGGER = LogManager.getLogger(WebGraph.class);
    private ElasticSearchService elasticSearchService;
    private AppConfiguration appConfiguration;

    WebGraph() throws IOException {
        elasticSearchService = new ElasticSearchService(ElasticSearchConfiguration.getInstance());
        appConfiguration = new AppConfiguration();
    }

    public void start() {
        LOGGER.info("reading pages from elastic");
        List<ElasticPage> elasticPages = getFromElastic();
        LOGGER.info("{} pages readed from elastic", elasticPages.size());
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
