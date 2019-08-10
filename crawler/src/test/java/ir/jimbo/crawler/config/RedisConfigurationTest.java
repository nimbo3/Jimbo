package ir.jimbo.crawler.config;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RedisConfigurationTest {

    private static RedisConfiguration configWithoutPath, configWithPath;

    @BeforeClass
    public static void init() throws IOException {
        configWithoutPath = new RedisConfiguration();
        configWithPath = new RedisConfiguration("src/test/resources/appConfig.properties");
    }

    @Test
    public void getNodes() {
        List<String> withPath = configWithPath.getNodes();
        List<String> withoutPath = configWithoutPath.getNodes();
        List<String> result = new ArrayList<>();
        result.add("redis://localhost:6380");
        Assert.assertArrayEquals(withPath.toArray(), result.toArray());
        Assert.assertArrayEquals(withoutPath.toArray(), result.toArray());
    }

    @Test
    public void isStandAlone() {
        boolean withPath = configWithPath.isStandAlone();
        boolean withoutPath = configWithoutPath.isStandAlone();
        Assert.assertTrue(withoutPath);
        Assert.assertTrue(withPath);
    }

    @Test
    public void getPassword() {
        String withPath = configWithPath.getPassword();
        String withoutPath = configWithoutPath.getPassword();
        Assert.assertEquals(withoutPath, "");
        Assert.assertEquals(withPath, "");
    }

    @Test
    public void getDomainExpiredTime() {
        int withPath = configWithPath.getDomainExpiredTime();
        int withoutPath = configWithoutPath.getDomainExpiredTime();
        Assert.assertEquals(withoutPath, 1000);
        Assert.assertEquals(withPath, 1000);
    }

    @Test
    public void getUrlExpiredTime() {
        int withPath = configWithPath.getUrlExpiredTime();
        int withoutPath = configWithoutPath.getUrlExpiredTime();
        Assert.assertEquals(withoutPath, 86400000);
        Assert.assertEquals(withPath, 86400000);
    }

}