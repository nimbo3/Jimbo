package ir.jimbo.pageprocessor.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import ir.jimbo.commons.exceptions.JimboException;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.model.Page;
import ir.jimbo.pageprocessor.config.ElasticSearchConfiguration;
import ir.jimbo.pageprocessor.config.HConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ElasticSearchService {
    private static final Logger LOGGER = LogManager.getLogger(HConfig.class);
    private ElasticSearchConfiguration configuration;
    private TransportClient client;
    private int requestTimeOutNanos;

    public ElasticSearchService(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
        requestTimeOutNanos = configuration.getRequestTimeOutNanos();
        client = configuration.getClient();
    }

    public boolean insertPages(List<Page> pages) {
        BulkRequest bulkRequest = new BulkRequest();
        String indexName = configuration.getIndexName();
        ObjectWriter writer = new ObjectMapper().writer();
        for (Page page : pages) {
            IndexRequest doc = new IndexRequest(indexName, "_doc", getMd5(page.getUrl()));
            byte[] bytes;
            try {
                bytes = writer.writeValueAsBytes(new ElasticPage(page));
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
            return !bulk.get().hasFailures();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("bulk insert has failures", e);
            return false;
        }
    }

    private String getMd5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");

            byte[] messageDigest = md.digest(input.getBytes());

            BigInteger no = new BigInteger(1, messageDigest);
            StringBuilder hashText = new StringBuilder(no.toString(16));
            while (hashText.length() < 32) {
                hashText.insert(0, "0");
            }
            return hashText.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new JimboException("fail in creating hash");
        }
    }
}