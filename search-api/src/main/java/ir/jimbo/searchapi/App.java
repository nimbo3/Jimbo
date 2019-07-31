package ir.jimbo.searchapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.manager.ElasticSearchService;
import ir.jimbo.searchapi.model.SearchQuery;
import ir.jimbo.searchapi.model.SearchResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

import static spark.Spark.get;
import static spark.Spark.port;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);
    private static ElasticSearchService elasticSearchService;
    static {
        ElasticSearchConfiguration elasticSearchConfiguration = null;
        try {
            elasticSearchConfiguration = ElasticSearchConfiguration.getInstance();
        } catch (IOException e) {
            LOGGER.error("config file is invalid" , e);
        }
        elasticSearchService = new ElasticSearchService(elasticSearchConfiguration);
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        port(1478);
        get("/search", (req, res) -> {
            String query = req.queryParams("q");
            LOGGER.info("query :" + query + " received.");
            if (req == null) {
                res.status(300);
                return "";
            }
            SearchQuery searchQuery = new SearchQuery(null, query, null);
            return objectMapper.writeValueAsString(elasticSearchService.getSearch(searchQuery));
        });
    }
}
