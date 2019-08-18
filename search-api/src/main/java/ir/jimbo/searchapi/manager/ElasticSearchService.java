package ir.jimbo.searchapi.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.model.SearchItem;
import ir.jimbo.searchapi.model.SearchResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
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
    private TransportClient client;
    private String pageIndexName;
    private ObjectMapper mapper;
    private Map<String, Float> fieldScores = Stream.of(new Object[][]{
            {"title", 1000.0F},
            {"url", 1.0F},
            {"metaTags", 1.0F},
            {"text", 1.0F},
            {"h1List", 1.0F},
            {"h2List", 1.0F},
            {"h3to6List", 1.0F},
    }).collect(Collectors.toMap(data -> (String) data[0], data -> (Float) data[1]));

    public ElasticSearchService(ElasticSearchConfiguration configuration) {
        client = configuration.getClient();
        mapper = new ObjectMapper();
        pageIndexName = configuration.getIndexName();
    }

    public SearchResult getSearch(BoolQueryBuilder query) {
        long startTime = System.currentTimeMillis();

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(pageIndexName);

        SearchResult searchResult = new SearchResult();

        SearchResponse searchResponse = searchRequestBuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(query)
                .setTimeout(TimeValue.timeValueMillis(100000))
                .setSize(10).setExplain(false).get();
        final String message = String.format("execute of query finished, query :%s", query.toString());
        LOGGER.info(message);

        parseSearchResults(searchResult, searchResponse);
        searchResult.setSearchTime(System.currentTimeMillis() - startTime);
        return searchResult;
    }

    private void parseSearchResults(SearchResult searchResult, SearchResponse searchResponse) {
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            try {
                ElasticPage page = mapper.readValue(hit.getSourceAsString(), ElasticPage.class);
                StringBuilder text = new StringBuilder();

                hit.getHighlightFields().forEach((fieldName, fieldValue) -> {
                    text.append(fieldValue.toString());
                    for (Text fragment : fieldValue.getFragments()) {
                        text.append(fragment.string()).append("<br/>");
                    }
                });

                searchResult.getSearchItemList().add(new SearchItem(page.getTitle(), text.toString(), page.getUrl()));
            } catch (IOException e) {
                LOGGER.error(String.format("error in parsing document :%s", hit.getSourceAsString()));
            }
        }
    }

    public TransportClient getClient() {
        return client;
    }
}