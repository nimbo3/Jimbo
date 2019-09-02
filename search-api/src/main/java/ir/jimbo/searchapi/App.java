package ir.jimbo.searchapi;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.manager.ElasticSearchService;
import ir.jimbo.searchapi.manager.HTableManager;
import ir.jimbo.searchapi.model.Graph;
import ir.jimbo.searchapi.model.Link;
import ir.jimbo.searchapi.model.Node;
import ir.jimbo.searchapi.model.SearchResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static spark.Spark.get;
import static spark.Spark.port;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);
    private static final String EXPRESSION = "([A-Za-z]*)";
    private static final String FUZZY_PATTERN = "\\?\"" + EXPRESSION + "\"\\?";
    private static final String MUST_PATTERN = "!\"" + EXPRESSION + "\"!";
    private static final String PREFIX_PATTERN = "~" + EXPRESSION + "~";
    private static final String EXACT_PATTERN = "=" + EXPRESSION + "=";
    private static final List<String> fields = new ArrayList<>(Arrays.asList("h1List", "h2List", "h3to6List",
            "metaTags", "text", "title", "url", "topAnchors"));
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
            LOGGER.info(String.format("Query: %s received.", query));
            Matcher fuzzyMatcher = fuzzyPattern.matcher(query);
            Matcher mustMatcher = mustPattern.matcher(query);
            Matcher prefixMatcher = prefixPattern.matcher(query);
            Matcher exactMatcher = exactPattern.matcher(query);
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            String normalQuery = query.replaceAll(FUZZY_PATTERN, "").replaceAll(MUST_PATTERN, "")
                    .replaceAll(PREFIX_PATTERN, "").replaceAll(EXACT_PATTERN, "");
            while (fuzzyMatcher.find()) {
                String fuzzyExp = fuzzyMatcher.group(1);
                if (!fuzzyExp.isEmpty())
                    for (String field : fields)
                        boolQuery = boolQuery.should(QueryBuilders.fuzzyQuery(field, fuzzyExp).fuzziness(Fuzziness.TWO))
                                ;
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
            if (!normalQuery.isEmpty())
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
        get("/graph", (req, res) -> {
            String url = req.queryParams("url");
            LOGGER.info("Getting neighbours of {}", url);
            HTableManager hTableManager = new HTableManager("l", "t",
                    "HBaseHealthChecker", MetricConfiguration.getInstance());
            LOGGER.info("HBase");
            Graph graph = new Graph();
            List<Node> nodes = new ArrayList<>();
            List<Link> links = new ArrayList<>();
            nodes.add(new Node(url));
            for (String node : elasticSearchService.getURLs(hTableManager.get(url))) {
                LOGGER.info("Neighbour: {}", node);
                nodes.add(new Node(node));
                links.add(new Link(node, url));
            }
            graph.setNodes(nodes);
            graph.setLinks(links);
            ObjectMapper mapper = new ObjectMapper().setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility
                    .ANY);
            final String json = mapper.writeValueAsString(graph);
            res.body(json);
            res.header("Access-Control-Allow-Origin", "*");
            return json;
        });
        get("/suggest", (req, res) -> {
            final String suggest = req.queryParams("suggest");
            LOGGER.info("Suggestion: {}", suggest);
            ObjectMapper mapper = new ObjectMapper().setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility
                    .ANY);
            final String json = mapper.writeValueAsString(elasticSearchService.suggest(suggest));
            res.body(json);
            res.header("Access-Control-Allow-Origin", "*");
            LOGGER.info(json);
            return json;
        });
        get("/top", (req, res) -> {
            final String category = req.queryParams("category");
            LOGGER.info("Top: {}", category);
            final String json = objectMapper.writeValueAsString(elasticSearchService.getTop(category));
            res.body(json);
            res.header("Access-Control-Allow-Origin", "*");
            LOGGER.info(json);
            return json;
        });
        get("/webgraph", (req, res) -> {
            LOGGER.info("Webgraph request");
            final String json = String.join("", Files.readAllLines(Paths.get("webgraph/graph.json"))).
                    replaceAll("\"src\"", "\"source\"").replaceAll("\"dst\"",
                    "\"target\"").replaceAll("\"pagerank\"", "\"size\"");
            res.body(json);
            res.header("Access-Control-Allow-Origin", "*");
            LOGGER.info(json);
            return json;
        });
    }
}
