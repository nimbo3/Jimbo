package ir.jimbo.searchapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.manager.ElasticSearchService;
import ir.jimbo.searchapi.model.SearchResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static spark.Spark.get;
import static spark.Spark.port;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);
    private static final String EXPRESSION = "([A-Za-z]*)";
    private static final String FUZZY_PATTERN = "\\?\"" + EXPRESSION + "\"\\?";
    private static final String MUST_PATTERN = "!\"" + EXPRESSION + "\"!";
    private static final String PREFIX_PATTERN = "~" + EXPRESSION + "~";
    private static final String EXACT_PATTERN = "=" + EXPRESSION + "=";
    private static final List<String> fields = new ArrayList<>(Arrays.asList("h1List", "h2List", "h3to6List",
            "metaTags", "text", "title", "url"));
    private static ElasticSearchService elasticSearchService;

    static {
        ElasticSearchConfiguration elasticSearchConfiguration = null;
        try {
            elasticSearchConfiguration = ElasticSearchConfiguration.getInstance();
        } catch (IOException e) {
            LOGGER.error("config file is invalid", e);
        }
        elasticSearchService = new ElasticSearchService(Objects.requireNonNull(elasticSearchConfiguration));
    }

    public static void main(String[] args) {
        Pattern fuzzyPattern = Pattern.compile(FUZZY_PATTERN);
        Pattern mustPattern = Pattern.compile(MUST_PATTERN);
        Pattern prefixPattern = Pattern.compile(PREFIX_PATTERN);
        Pattern exactPattern = Pattern.compile(EXACT_PATTERN);
        ObjectMapper objectMapper = new ObjectMapper();
        port(1478);
        get("/search", (req, res) -> {
            String query = req.queryParams("q");
            LOGGER.info(String.format("query :%s received.", query));
            Matcher fuzzyMatcher = fuzzyPattern.matcher(query);
            Matcher mustMatcher = mustPattern.matcher(query);
            Matcher prefixMatcher = prefixPattern.matcher(query);
            Matcher exactMatcher = exactPattern.matcher(query);
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            String normalQuery = query.replaceAll(FUZZY_PATTERN, "").replaceAll(MUST_PATTERN, "")
                    .replaceAll(PREFIX_PATTERN, "");
            while (fuzzyMatcher.find()) {
                String fuzzyExp = fuzzyMatcher.group(1);
                if (!fuzzyExp.isEmpty())
                    for (String field : fields)
                        boolQuery = boolQuery.should(QueryBuilders.fuzzyQuery(field, fuzzyExp));
            }
            while (mustMatcher.find()) {
                String mustExp = mustMatcher.group(1);
                if (!mustExp.isEmpty())
                    boolQuery = boolQuery.must(QueryBuilders.multiMatchQuery(mustExp, fields.toArray(new String
                            []{})));
            }
            while (prefixMatcher.find()) {
                String prefixExp = prefixMatcher.group(1);
                if (!prefixExp.isEmpty())
                    for (String field : fields)
                        boolQuery = boolQuery.should(QueryBuilders.prefixQuery(field, prefixExp));
            }
            while (exactMatcher.find()) {
                String exactExp = exactMatcher.group(1);
                if (!exactExp.isEmpty())
                    for (String field : fields)
                        boolQuery = boolQuery.should(QueryBuilders.termQuery(field + ".keyword", exactExp));
            }
            boolQuery = boolQuery.should(QueryBuilders.multiMatchQuery(normalQuery, fields.toArray(new String[]{})));
            final String lang = req.queryParams("lang");
            if (lang != null && !lang.isEmpty())
                boolQuery = boolQuery.filter(QueryBuilders.termQuery("lang", lang));
            final String category = req.queryParams("category");
            if (category != null && !category.isEmpty())
                boolQuery = boolQuery.filter(QueryBuilders.termQuery("category", category));
            SearchResult search = elasticSearchService.getSearch(boolQuery);
            String responseText = objectMapper.writeValueAsString(search);
            res.body(responseText);
            res.header("Access-Control-Allow-Origin", "*");
            return responseText;
        });
    }
}
