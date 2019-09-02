package ir.jimbo.searchapi.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.model.SearchItem;
import ir.jimbo.searchapi.model.SearchResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;

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
        HighlightBuilder highlightQuery = new HighlightBuilder()
                .numOfFragments(3)
                .fragmentSize(150)
                .field("text")
                .preTags("")
                .postTags("");
        SearchResponse searchResponse = client
                .prepareSearch(pageIndexName).highlighter(highlightQuery)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.functionScoreQuery(query, new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunction("rank")
                                .missing(1)
                                .modifier(FieldValueFactorFunction.Modifier.SQRT))}))
                .setTimeout(TimeValue.timeValueMillis(100000))
                .setSize(10).setExplain(false).get();
        final String message = String.format("Execution of query finished, query :%s", query.toString());
        LOGGER.info(message);
        SearchResult searchResult = new SearchResult();
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
                    for (Text fragment : fieldValue.getFragments())
                        text.append(fragment.string());
                });
                LOGGER.info("URL: {}", page.getUrl());
                LOGGER.info("Score: {}", hit.getScore());
                searchResult.getSearchItemList().add(new SearchItem(page.getTitle(), text.toString(), page.getUrl()));
            } catch (IOException e) {
                LOGGER.error(String.format("error in parsing document :%s", hit.getSourceAsString()));
            }
        }
    }

    public List<String> getURLs(List<String> ids) {
        try {
            final IdsQueryBuilder query = QueryBuilders.idsQuery();
            for (String id : ids)
                query.addIds(id);
            final String message = query.toString();
            LOGGER.info(message);
            SearchResponse searchResponse = client
                    .prepareSearch(pageIndexName)
                    .setQuery(query)
                    .setTimeout(TimeValue.timeValueMillis(100000))
                    .setExplain(false)
                    .get();
            LOGGER.info("Query timeout: {}", searchResponse.isTimedOut());
            LOGGER.info("Query finished");
            ObjectMapper objectMapper = new ObjectMapper();
            return Arrays.stream(searchResponse.getHits().getHits()).map(o -> {
                try {
                    return objectMapper.readValue(o.getSourceAsString(), ElasticPage.class).getUrl();
                } catch (IOException e) {
                    LOGGER.error("Parsing error: ", e);
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
        } catch (Exception e) {
            LOGGER.error("Query error: ", e);
        }
        return new ArrayList<>();
    }

    public List<String> suggest(String exp) {
        return client.prepareSearch(pageIndexName).suggest(new SuggestBuilder().addSuggestion("suggestion",
                SuggestBuilders.completionSuggestion("suggest").prefix(exp, Fuzziness.TWO).skipDuplicates(true
                ).size(5))).get(TimeValue.timeValueMillis(100000)).getSuggest().getSuggestion("suggestion").
                getEntries().stream().flatMap(o -> o.getOptions().stream()).map(o -> o.getText().toString()).collect(
                Collectors.toList());
    }

    public TransportClient getClient() {
        return client;
    }

    public SearchResult getTop(String category) {
        long time = System.currentTimeMillis();
        return new SearchResult(Arrays.stream(client.prepareSearch(pageIndexName).setQuery(QueryBuilders.boolQuery().
                filter(QueryBuilders.termQuery("category", category))).setExplain(false).setSize(100).addSort(
                "rank", SortOrder.DESC).get(TimeValue.timeValueMillis(100000)).getHits().getHits()).map(hit -> {
            try {
                final ElasticPage page = mapper.readValue(hit.getSourceAsString(), ElasticPage.class);
                return new SearchItem(page.getTitle(), (page.getText().length() > 100 ? page.getText().substring(0, 100)
                        : page.getText()) , page.getUrl());
            } catch (IOException e) {
                LOGGER.error("Page parse error", e);
                return null;
            }
        }).collect(Collectors.toList()), System.currentTimeMillis() - time);
    }
}