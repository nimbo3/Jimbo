package ir.jimbo.espagemigrator.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.espagemigrator.config.ElasticSearchConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.language.detect.LanguageDetector;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ElasticSearchService {
    private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);
    private static final int SCROLL_SIZE = 10;
    private static final long SCROLL_TIMEOUT = 1;
    private ElasticSearchConfiguration configuration;
    private TransportClient client;
    private int requestTimeOutNanos;
    private LanguageDetector languageDetector;
    private HashUtil hashUtil;
    private String esScrollID = null;

    public ElasticSearchService(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
        languageDetector = LanguageDetector.getDefaultLanguageDetector();
        try {
            languageDetector.loadModels();
        } catch (IOException e) {//we trust that it never happens
            LOGGER.error("error in loading lang detector modules; ", e);
        }
        hashUtil = new HashUtil();
        requestTimeOutNanos = configuration.getRequestTimeOutNanos();
        client = configuration.getClient();
    }

    public boolean insertPages(List<ElasticPage> pages) {
        BulkRequest bulkRequest = new BulkRequest();
        String indexName = configuration.getIndexName();
        ObjectWriter writer = new ObjectMapper().writer();
        for (ElasticPage elasticPage : pages) {
            IndexRequest doc = new IndexRequest(indexName, "_doc", hashUtil.getMd5(elasticPage.getUrl()));
            byte[] bytes;
            try {
                languageDetector.reset();
                languageDetector.addText(elasticPage.getText());
                elasticPage.setLang(languageDetector.detect().getLanguage());
                bytes = writer.writeValueAsBytes(elasticPage);
            } catch (JsonProcessingException e) {
                LOGGER.error("error in parsing page with url with jackson:" + elasticPage.getUrl(), e);
                continue;
            }
            doc.source(bytes, XContentType.JSON);
            bulkRequest.add(doc);
        }
        if (bulkRequest.requests().isEmpty())
            return true;
        bulkRequest.timeout(TimeValue.timeValueNanos(requestTimeOutNanos));
        ActionFuture<BulkResponse> bulk = client.bulk(bulkRequest);
        try {
            BulkResponse bulkItemResponses = bulk.get();
            if (!bulkItemResponses.hasFailures())
                return true;
            else {
                final String message = bulkItemResponses.buildFailureMessage();
                LOGGER.error(message);
                for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                    LOGGER.info(bulkItemResponse.getResponse().getResult());
                }
                return false;
            }
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("bulk insert has failures", e);
            return false;
        }
    }

    public synchronized List<ElasticPage> getSourcePages() {
        SearchResponse scrollResp;
        if (esScrollID == null)
            scrollResp = client.prepareSearch(configuration.getSourceName())
                    .setScroll(TimeValue.timeValueMinutes(SCROLL_TIMEOUT))
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(SCROLL_SIZE)
                    .get();
        else
            scrollResp = client.prepareSearchScroll(esScrollID).setScroll(TimeValue.timeValueMinutes(SCROLL_TIMEOUT)).execute().actionGet();
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

    public TransportClient getClient() {
        return client;
    }
}