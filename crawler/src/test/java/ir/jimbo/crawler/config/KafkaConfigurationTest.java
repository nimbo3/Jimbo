package ir.jimbo.crawler.config;


import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class KafkaConfigurationTest {

    private static KafkaConfiguration configWithoutPath, configWithPath;

    @BeforeClass
    public static void init() throws IOException {
        configWithoutPath = new KafkaConfiguration();
        configWithPath = new KafkaConfiguration("src/test/resources/kafkaConfig.properties");
    }

    @Test
    public void getPollDuration() {
        int withPath = configWithPath.getPollDuration();
        int withoutPath = configWithoutPath.getPollDuration();
        Assert.assertEquals(withPath, 10000);
        Assert.assertEquals(withoutPath, 10000);
    }

    @Test
    public void getLinkTopicName() {
        String withPath = configWithPath.getLinkTopicName();
        String withoutPath = configWithoutPath.getLinkTopicName();
        Assert.assertEquals(withPath, "links");
        Assert.assertEquals(withoutPath, "links");
    }

    @Test
    public void getPageTopicName() {
        String withPath = configWithPath.getPageTopicName();
        String withoutPath = configWithoutPath.getPageTopicName();
        Assert.assertEquals(withPath, "pages");
        Assert.assertEquals(withoutPath, "pages");
    }
}