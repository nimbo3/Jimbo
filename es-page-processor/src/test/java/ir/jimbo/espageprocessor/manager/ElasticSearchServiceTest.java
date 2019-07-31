package ir.jimbo.espageprocessor.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.ElasticPage;
import ir.jimbo.commons.model.HtmlTag;
import ir.jimbo.commons.model.Page;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.espageprocessor.config.ElasticSearchConfiguration;
import org.elasticsearch.action.get.GetResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.IndexSettings;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        ElasticSearchConfiguration elasticSearchConfiguration = ElasticSearchConfiguration.getInstance();
        elasticSearchService = new ElasticSearchService(elasticSearchConfiguration);
        hashUtil = new HashUtil();
    }

    @After
    public void close() {
        embeddedElastic.stop();
    }

    @Test
    public void testInsertPages() throws Exception {
        List<Page> pages = Arrays.asList(new Page("url", "test", Collections.EMPTY_LIST
                , Arrays.asList(new HtmlTag("test", "http://test.com")), Arrays.asList(new HtmlTag("", "h1"))
                , Arrays.asList(new HtmlTag("", "h2"))
                , Arrays.asList(new HtmlTag("", "h3")), Arrays.asList(new HtmlTag("test_text"))));
        boolean result = elasticSearchService.insertPages(pages);

        Assert.assertEquals(true, result);

        ObjectMapper mapper = new ObjectMapper();
        GetResponse documentFields = elasticSearchService.getClient().prepareGet().setIndex("page").setId(hashUtil.getMd5("url")).setType("_doc").get();
        ElasticPage elasticPage = mapper.readValue(documentFields.getSourceAsString(), ElasticPage.class);
        Page page = pages.get(0);
        Assert.assertEquals(page.getH1List().get(0).getContent(), elasticPage.getH1List().get(0));
        Assert.assertEquals(page.getH2List().get(0).getContent(), elasticPage.getH2List().get(0));
        Assert.assertEquals(page.getH3to6List().get(0).getContent(), elasticPage.getH3to6List().get(0));
        Assert.assertEquals(page.getMetadata().size(), elasticPage.getMetaTags().size());
        Assert.assertEquals(page.getTitle(), elasticPage.getTitle());

    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme