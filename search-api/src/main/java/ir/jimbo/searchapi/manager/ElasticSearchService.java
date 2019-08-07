package ir.jimbo.searchapi.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.model.SearchItem;
import ir.jimbo.searchapi.model.SearchQuery;
import ir.jimbo.searchapi.model.SearchResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ElasticSearchService {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);
    private ElasticSearchConfiguration configuration;
    private TransportClient client;
    private int requestTimeOutNanos;
    private String pageIndexName;
    private ObjectMapper mapper;
    private List<String> fieldNames = new ArrayList<>(Arrays.asList("h1List", "h2List", "h3to6List", "metaTags", "text", "title", "url"));
    private Map<String, Float> filedScores = Stream.of(new Object[][]{
            {"title", 1000},
            {"url", 1},
            {"metaTags", 1},
            {"text", 1},
            {"h1List", 1},
            {"h2List", 1},
            {"h3to6List", 1},
    }).collect(Collectors.toMap(data -> (String) data[0], data -> (Float) data[1]));

    public ElasticSearchService(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
        requestTimeOutNanos = configuration.getRequestTimeOutNanos();
        client = configuration.getClient();
        mapper = new ObjectMapper();
        pageIndexName = configuration.getIndexName();
    }

    public SearchResult getSearch(SearchQuery query) {
        long startTime = System.currentTimeMillis();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(pageIndexName);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        SearchResult searchResult = new SearchResult();

        LOGGER.info("start to create Query for :" + query.toString());
        if (query.getFuzzyQuery() != null && !query.getFuzzyQuery().trim().equals("")) {
            filedScores.forEach((fieldName, weight) -> {
                FuzzyQueryBuilder queryBuilder = new FuzzyQueryBuilder(fieldName, query.getFuzzyQuery());
                boolQueryBuilder.should(queryBuilder);
            });
        }
        if (query.getExactQuery() != null && !query.getExactQuery().trim().equals("")) {
            BoolQueryBuilder mustBoolQuery = new BoolQueryBuilder();
            for (String fieldName : fieldNames) {
                mustBoolQuery.should(new TermQueryBuilder(fieldName , query.getExactQuery()));
            }
            boolQueryBuilder.must(mustBoolQuery);
        }
        if (query.getNormalQuery() != null && !query.getNormalQuery().trim().equals("")) {
            for (String fieldName : fieldNames) {
                boolQueryBuilder.should(new MatchQueryBuilder(fieldName, query.getNormalQuery()));
            }
        }
        SearchResponse searchResponse = searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder)
                .setTimeout(TimeValue.timeValueMillis(100000))
                .setSize(10).setExplain(false).get();
        LOGGER.info("execute of query finished, query :" + query.toString());

        parseSearchResults(searchResult, searchResponse);
        searchResult.setSearchTime(System.currentTimeMillis() - startTime);
        return searchResult;
    }

    private void parseSearchResults(SearchResult searchResult, SearchResponse searchResponse) {
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            try {
                ElasticPage page = mapper.readValue(hit.getSourceAsString(), ElasticPage.class);
                String text = "";
                if (page.getText().length() > 250) {
                    text = page.getText().substring(0, 250);
                } else {
                    text = page.getText();
                }
                searchResult.getSearchItemList().add(new SearchItem(page.getTitle(), text, page.getUrl()));
            } catch (IOException e) {
                LOGGER.error("error in parsing document :" + hit.getSourceAsString());
            }
        }
    }

    public TransportClient getClient() {
        return client;
    }
}