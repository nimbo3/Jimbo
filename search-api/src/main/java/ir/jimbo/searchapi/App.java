package ir.jimbo.searchapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.manager.ElasticSearchService;
import ir.jimbo.searchapi.model.SearchQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static spark.Spark.get;
import static spark.Spark.port;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);//todo regex has some bugs
    private static final String FUZZY_PATTERN = "\\?\"([A-Za-z]*)\"\\?";
    private static final String MUST_PATTERN = "!\"([A-Za-z]*)\"!";
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
        Pattern fuzzyPattern = Pattern.compile(FUZZY_PATTERN);
        Pattern mustPattern = Pattern.compile(MUST_PATTERN);
        ObjectMapper objectMapper = new ObjectMapper();
        port(1478);
        get("/search", (req, res) -> {
            String query = req.queryParams("q");
            LOGGER.info("query :" + query + " received.");
            Matcher fuzzyMatcher = fuzzyPattern.matcher(query);
            Matcher mustMatcher = mustPattern.matcher(query);
            StringBuilder fuzzyQuery = new StringBuilder();
            StringBuilder mustQuery = new StringBuilder();
            String normalQuery = query;
            if (fuzzyMatcher.find()) {
                for (int i = 0; i < fuzzyMatcher.groupCount(); i++) {
                    fuzzyQuery.append(fuzzyMatcher.group(i).replaceAll("\\?\"", "")
                            .replaceAll("\"\\?", ""));
                    normalQuery = normalQuery.replace("\\?\""+ fuzzyMatcher.group(i) + "\\?\"", "");
                }
            }
            if (mustMatcher.find()) {
                for (int i = 0; i < mustMatcher.groupCount(); i++) {
                    mustQuery.append(mustMatcher.group(i).replaceAll("!\"" , "")
                            .replaceAll("\"!", ""));
                    normalQuery = normalQuery.replace("\""+ mustMatcher.group(i) + "\"", "");
                }
            }
            if (req == null) {
                res.status(300);
                return "";
            }

            SearchQuery searchQuery = new SearchQuery(mustQuery.toString(), normalQuery, fuzzyQuery.toString());
            String responseText = objectMapper.writeValueAsString(elasticSearchService.getSearch(searchQuery));
            res.body(responseText);
            res.header("Access-Control-Allow-Origin", "*");
            return responseText;
        });
    }
}
