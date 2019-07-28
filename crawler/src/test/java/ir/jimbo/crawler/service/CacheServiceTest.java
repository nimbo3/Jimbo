package ir.jimbo.crawler.service;


import ir.jimbo.crawler.config.RedisConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.io.IOException;

public class CacheServiceTest {

    private CacheService cacheService;
    private RedisServer redisServer;
    @Before
    public void setUp() throws IOException {
        //todo initialize
        redisServer = new RedisServer(6379);
        redisServer.start();
        RedisConfiguration redisConfiguration = new RedisConfiguration();
        cacheService = new CacheService(redisConfiguration);
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
    }

    @Test
    public void testIsUrlExists() throws Exception {
        cacheService.addUrl("uri");
        boolean result = cacheService.isUrlExists("uri");
        Assert.assertEquals(true, result);
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme