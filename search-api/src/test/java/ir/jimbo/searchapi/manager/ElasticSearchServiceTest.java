package ir.jimbo.searchapi.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.searchapi.config.ElasticSearchConfiguration;
import ir.jimbo.searchapi.model.SearchItem;
import ir.jimbo.searchapi.model.SearchResult;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.IndexSettings;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ElasticSearchServiceTest {
    private static final List<String> fields = new ArrayList<>(Arrays.asList("h1List", "h2List", "h3to6List",
            "metaTags", "text", "title"));
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
                , Collections.singletonList(new HtmlTag("test", "http://test.com")), Collections.singletonList(new HtmlTag("", "h1"))
                , Collections.singletonList(new HtmlTag("", "h2"))
                , Collections.singletonList(new HtmlTag("", "h3")), Collections.singletonList(new HtmlTag("test_text", "test_text")));

        IndexRequest doc = new IndexRequest("page", "_doc", hashUtil.getMd5("url"));
        byte[] bytes;
        ElasticPage elasticPage = new ElasticPage(page);
        elasticPage.setLang("test");
        elasticPage.setCategory("test");
        bytes = objectMapper.writeValueAsBytes(elasticPage);
        doc.source(bytes, XContentType.JSON);
        elasticSearchService.getClient().index(doc);
        Thread.sleep(10000);
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("test", fields.
                toArray(new String[]{})));
        for (String field : fields)
            boolQuery.should(QueryBuilders.fuzzyQuery(field, "test"));
        for (String field : fields)
            boolQuery.should(QueryBuilders.prefixQuery(field, "test"));
        for (String field : fields)
            boolQuery.should(QueryBuilders.termQuery(field + ".keyword", "test"));
        boolQuery.should(QueryBuilders.multiMatchQuery("test", fields.toArray(new String[]{})));
        boolQuery.filter(QueryBuilders.termQuery("lang", "test"));
        boolQuery.filter(QueryBuilders.termQuery("category", "test"));
        SearchResult result = elasticSearchService.getSearch(boolQuery);
        Assert.assertArrayEquals(Collections.singletonList(new SearchItem("test", "test_text", "url")).toArray(), result.getSearchItemList().toArray());
    }
}
