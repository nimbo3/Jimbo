package ir.jimbo.web.graph.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.web.graph.config.ElasticSearchConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearchService {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);
    private static final int SCROLL_SIZE = 10;
    private static final long SCROLL_TIMEOUT = 2;
    private ElasticSearchConfiguration configuration;
    private TransportClient client;
    private String esScrollID = null;
    private MultiGetRequestBuilder multiGetRequestBuilder;

    public ElasticSearchService(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
        client = configuration.getClient();
    }

    public List<ElasticPage> getSourcePages() {
        SearchResponse scrollResp;
        if (esScrollID == null)
            scrollResp = client.prepareSearch(configuration.getSourceName())
                    .setScroll(TimeValue.timeValueMinutes(SCROLL_TIMEOUT))
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(SCROLL_SIZE)
                    .get();
        else
            scrollResp = client.prepareSearchScroll(esScrollID)
                    .setScroll(TimeValue.timeValueMinutes(SCROLL_TIMEOUT))
                    .execute()
                    .actionGet();
        esScrollID = scrollResp.getScrollId();
        List<ElasticPage> pages = new ArrayList<>();
        SearchHit[] searchHits = scrollResp.getHits().getHits();
        ObjectMapper reader = new ObjectMapper();
        for (SearchHit hit : searchHits) {
            try {
                pages.add(reader.readValue(hit.getSourceAsString(), ElasticPage.class));
            } catch (IOException e) {
                LOGGER.error("Source page parse exception", e);
            }
        }
        return pages;
    }

    public ElasticPage getDocument(String id) throws IOException {
        GetRequestBuilder doc = client.prepareGet(configuration.getSourceName(), "_doc", id);
        ObjectMapper reader = new ObjectMapper();
        String sourceAsString = doc.get().getSourceAsString();
        if (sourceAsString == null) {
            return null;
        } else {
            return reader.readValue(sourceAsString, ElasticPage.class);
        }
    }

    public MultiGetRequestBuilder getMultiGetRequestBuilder() {
        if (multiGetRequestBuilder == null) {
            return multiGetRequestBuilder = client.prepareMultiGet();
        }
        return multiGetRequestBuilder;
    }

    public List<ElasticPage> getDocuments(MultiGetRequestBuilder multiGetRequestBuilder) {
        List<ElasticPage> elasticPages = new ArrayList<>();
        if (multiGetRequestBuilder == null) {
            LOGGER.error("multiGetRequestBuilder is null. nothing will return");
        } else {
            MultiGetResponse multiGetItemResponses = multiGetRequestBuilder.get();
            ObjectMapper reader = new ObjectMapper();
            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                GetResponse response = itemResponse.getResponse();
                if (response.isExists()) {
                    String json = response.getSourceAsString();
                    try {
                        elasticPages.add(reader.readValue(json, ElasticPage.class));
                    } catch (IOException e) {
                        LOGGER.error("exception in converting json to elasticPage instance." +
                                " source as string : {}", json, e);
                    }
                }
            }
        }
        return elasticPages;
    }

    public List<ElasticPage> getSourcePagesSorted() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchResponse response = client.prepareSearch("page_rank").setTypes("_doc")
                .addSort(SortBuilders.fieldSort("rank")
                        .order(SortOrder.DESC)).setQuery(qb)
                .setSize(100).execute().actionGet();

        for(SearchHit hits : response.getHits())
        {
            System.out.print("id = " + hits.getId());
            System.out.println(hits.getSourceAsString());
        }
        return null;
    }
}