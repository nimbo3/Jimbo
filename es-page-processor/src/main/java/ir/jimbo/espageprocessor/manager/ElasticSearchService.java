package ir.jimbo.espageprocessor.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.model.Page;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.espageprocessor.config.ElasticSearchConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.language.detect.LanguageDetector;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ElasticSearchService {
        private static final Logger LOGGER = LogManager.getLogger(ElasticSearchService.class);
    private ElasticSearchConfiguration configuration;
    private TransportClient client;
    private int requestTimeOutNanos;
    private LanguageDetector languageDetector;
    private HashUtil hashUtil;
    public ElasticSearchService(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
        languageDetector = LanguageDetector.getDefaultLanguageDetector();
        try {
            languageDetector.loadModels();
        } catch (IOException e) {//we trust that it never happens
            LOGGER.error("error in loading lang detector modules; " , e);
        }
        hashUtil = new HashUtil();
        requestTimeOutNanos = configuration.getRequestTimeOutNanos();
        client = configuration.getClient();
    }

    public boolean insertPages(List<Page> pages) {
        BulkRequest bulkRequest = new BulkRequest();
        String indexName = configuration.getIndexName();
        ObjectWriter writer = new ObjectMapper().writer();
        for (Page page : pages) {
            IndexRequest doc = new IndexRequest(indexName, "_doc", hashUtil.getMd5(page.getUrl()));
            byte[] bytes;
            try {
                languageDetector.reset();
                ElasticPage elasticPage = new ElasticPage(page);
                languageDetector.addText(elasticPage.getText());
                elasticPage.setLang(languageDetector.detect().getLanguage());
                bytes = writer.writeValueAsBytes(elasticPage);
            } catch (JsonProcessingException e) {
                LOGGER.error("error in parsing page with url with jackson:" + page.getUrl(), e);
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
                LOGGER.error(bulkItemResponses.buildFailureMessage());
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

    public TransportClient getClient() {
        return client;
    }
}