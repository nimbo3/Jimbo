package ir.jimbo.crawler.service;


import ir.jimbo.crawler.config.RedisConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RSetCache;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

/**
 * class for connecting to redis database (LRU cache)
 */
public class CacheService {

    private Logger logger = LogManager.getLogger(this.getClass());
    private Config config;
    private RedissonClient redissonClient;
    private RSetCache<Object> domains;
    private int expiredTimeDomainSecond;

    public CacheService(RedisConfiguration redisConfiguration) {
        config = new Config();
        if (redisConfiguration.isStandAlone()) {
            config.useSingleServer().setAddress("redis://" + redisConfiguration.getNodes().get(0))
                    .setPassword(redisConfiguration.getPassword());
        } else {
            ClusterServersConfig clusterServersConfig = config.useClusterServers();
            clusterServersConfig.setScanInterval(200);
            for (String node : redisConfiguration.getNodes()) {
                clusterServersConfig.addNodeAddress("redis://" + node);
            }
            clusterServersConfig.setPassword(redisConfiguration.getPassword());
        }
        redissonClient = Redisson.create(config);
        expiredTimeDomainSecond = redisConfiguration.getExpiredTime();
        domains = redissonClient.getSetCache(redisConfiguration.getSetName());
        logger.info("redis connection created.");
    }

    public void addDomain(String domain) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        // the result is for logging
        boolean result = domains.add(domain, expiredTimeDomainSecond, TimeUnit.SECONDS);

        if (!result) {
            logger.error("domain add to redis failed. domain : " + domain);
        }
    }

    public boolean isDomainExist(String key) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        return domains.contains(key);
    }
}
