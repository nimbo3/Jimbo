package ir.jimbo.crawler.config;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class AppConfigurationTest {

    private static AppConfiguration configWithoutPath, configWithPath;

    @BeforeClass
    public static void init() throws IOException {
        configWithoutPath = new AppConfiguration();
        configWithPath = new AppConfiguration("src/test/resources/appConfig.properties");
    }

    @Test
    public void getPageParserSize() {
        int withPath = configWithPath.getPageParserSize();
        int withoutPath = configWithoutPath.getPageParserSize();
        Assert.assertEquals(withoutPath, 150);
        Assert.assertEquals(withPath, 150);
    }

    @Test
    public void getLinkConsumerSize() {
        int withPath = configWithPath.getLinkConsumerSize();
        int withoutPath = configWithoutPath.getLinkConsumerSize();
        Assert.assertEquals(withoutPath, 2);
        Assert.assertEquals(withPath, 2);
    }

    @Test
    public void getQueueSize() {
        int withPath = configWithPath.getQueueSize();
        int withoutPath = configWithoutPath.getQueueSize();
        Assert.assertEquals(withoutPath, 1000);
        Assert.assertEquals(withPath, 1000);
    }

}