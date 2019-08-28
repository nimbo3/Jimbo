package ir.jimbo.crawler.service;


import ir.jimbo.commons.config.MetricConfiguration;
import ir.jimbo.crawler.config.RedisConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisServer;

import java.io.IOException;

public class CacheServiceTest {

    private CacheService cacheService;
    private RedisServer redisServer;

    @Before
    public void setUp() throws IOException {
        MetricConfiguration metrics = MetricConfiguration.getInstance();
        redisServer = new RedisServer(6380);
        redisServer.start();
        RedisConfiguration redisConfiguration = new RedisConfiguration();
        cacheService = new CacheService(redisConfiguration);
    }

    @After
    public void close() {
        redisServer.stop();
    }

    @Test
    public void testIsDomainExist() {
        cacheService.addDomain("domain");
        boolean result = cacheService.isDomainExist("domain");
        Assert.assertTrue(result);

        result = cacheService.isDomainExist("domain2");
        Assert.assertFalse(result);
    }

    @Test
    public void testIsUrlExists() {
        cacheService.addUrl("uri");
        boolean result = cacheService.isUrlExists("uri");
        Assert.assertTrue(result);

        result = cacheService.isUrlExists("uri2");
        Assert.assertFalse(result);
    }

    @Test
    public void testThirtySecondCache() throws InterruptedException {
        cacheService.addDomain("domain");
        Thread.sleep(1500);
        boolean result = cacheService.isDomainExist("domain");
        Assert.assertFalse(result);
    }

    @Test
    public void testEmptyDomain() {
        cacheService.addDomain("");
        boolean result = cacheService.isDomainExist("");
        Assert.assertTrue(result);
    }

    @Test
    public void testEmptyUrl() {
        cacheService.addUrl("   ");
        boolean result = cacheService.isUrlExists("   ");
        Assert.assertTrue(result);
    }
}

