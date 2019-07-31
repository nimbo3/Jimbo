package ir.jimbo.crawler.service;


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
        redisServer = new RedisServer(6379);
        redisServer.start();
        RedisConfiguration redisConfiguration = new RedisConfiguration();
        cacheService = new CacheService(redisConfiguration, "test");
    }

    @After
    public void close() {
        redisServer.stop();
    }

    @Test
    public void testIsDomainExist() throws Exception {
        cacheService.addDomain("domain");
        boolean result = cacheService.isDomainExist("domain");
        Assert.assertEquals(true, result);


        result = cacheService.isDomainExist("domain2");
        Assert.assertEquals(false, result);
    }

    @Test
    public void testIsUrlExists() throws Exception {
        cacheService.addUrl("uri");
        boolean result = cacheService.isUrlExists("uri");
        Assert.assertEquals(true, result);

        result = cacheService.isUrlExists("uri2");
        Assert.assertEquals(false, result);
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme