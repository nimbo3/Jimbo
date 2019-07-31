package ir.jimbo.searchapi.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.model.SearchQuery;
import ir.jimbo.searchapi.model.SearchResult;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.IndexSettings;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class ElasticSearchServiceTest {
    private ElasticSearchService elasticSearchService;
    private HashUtil hashUtil;
    private EmbeddedElastic embeddedElastic;
    @Before
    public void setUp() throws IOException, InterruptedException {
        embeddedElastic = EmbeddedElastic.builder().withDownloadDirectory(new File("./embeddedEs")).withDownloaderReadTimeout(10, TimeUnit.MINUTES)
                .withStartTimeout(10, TimeUnit.MINUTES)
                .withElasticVersion("6.3.2")
                .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9300)
                .withSetting(PopularProperties.CLUSTER_NAME, "jimbo")
                .withIndex("page", IndexSettings.builder()
                        .build())
                .build()
                .start();

        hashUtil = new HashUtil();
        ElasticSearchConfiguration elasticSearchConfiguration = ElasticSearchConfiguration.getInstance();
        elasticSearchService = new ElasticSearchService(elasticSearchConfiguration);
    }

    @After
    public void close() {
        embeddedElastic.stop();
    }

    @Test
    public void testGetSearch() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        Page page = new Page("url", "test", Collections.EMPTY_LIST
                , Arrays.asList(new HtmlTag("test", "http://test.com")), Arrays.asList(new HtmlTag("", "h1"))
                , Arrays.asList(new HtmlTag("", "h2"))
                , Arrays.asList(new HtmlTag("", "h3")), Arrays.asList(new HtmlTag("test_text", "test_text")));

        IndexRequest doc = new IndexRequest("page", "_doc", hashUtil.getMd5("url"));
        byte[] bytes;
        ElasticPage elasticPage = new ElasticPage(page);
        bytes = objectMapper.writeValueAsBytes(elasticPage);
        doc.source(bytes, XContentType.JSON);
        elasticSearchService.getClient().index(doc);
        Thread.sleep(10000);
        List<SearchResult> result = elasticSearchService.getSearch(new SearchQuery("test", "test", "test"));
        Assert.assertArrayEquals(Collections.<SearchResult>singletonList(new SearchResult("test", "test_text", "url")).toArray(), result.toArray());
    }
}
