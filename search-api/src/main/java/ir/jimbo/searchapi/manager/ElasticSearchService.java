package ir.jimbo.searchapi.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.model.SearchQuery;
import ir.jimbo.searchapi.model.SearchResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ElasticSearchService {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);
    private ElasticSearchConfiguration configuration;
    private TransportClient client;
    private int requestTimeOutNanos;
    private String pageIndexName;
    private ObjectMapper mapper;
    private List<String> fieldNames = new ArrayList<>(Arrays.asList("h1List", "h2List", "h3to6List", "metaTags", "text", "title", "url"));

    public ElasticSearchService(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
        requestTimeOutNanos = configuration.getRequestTimeOutNanos();
        client = configuration.getClient();
        mapper = new ObjectMapper();
//        objectReader = objectMapper.readerFor(ElasticPage.class);
        pageIndexName = configuration.getIndexName();
    }

    public List<SearchResult> getSearch(SearchQuery query) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(pageIndexName);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        List<SearchResult> searchResults = new ArrayList<>();
        LOGGER.info("start to create Query for :" + query.toString());
        if (query.getFuzzyQuery() != null) {
            boolQueryBuilder.should(new FuzzyQueryBuilder("title", query.getFuzzyQuery()));
        }

        if (query.getExactQuery() != null) {
            boolQueryBuilder.must(new TermQueryBuilder("title", query.getExactQuery()));
        }
        if (query.getNormalQuery() != null) {
            boolQueryBuilder.should(new MatchQueryBuilder("title", query.getNormalQuery()));
        }
        SearchResponse searchResponse = searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder)
                .setTimeout(TimeValue.timeValueMillis(10000))
                .setSize(50).setExplain(false).get();
        LOGGER.info("execute of query finished, query :" + query.toString());

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            try {
                ElasticPage page = mapper.readValue(hit.getSourceAsString(), ElasticPage.class);
                String text = "";
                if (page.getText().length() > 250) {
                    text = page.getText().substring(0, 250);
                } else {
                    text = page.getText();
                }
                searchResults.add(new SearchResult(page.getTitle(), text, page.getUrl()));
            } catch (IOException e) {
                LOGGER.error("error in parsing document :" + hit.getSourceAsString());
            }
        }
        return searchResults;
    }
}