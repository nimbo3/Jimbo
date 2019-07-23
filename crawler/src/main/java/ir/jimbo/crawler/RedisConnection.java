package ir.jimbo.crawler;


import ir.jimbo.crawler.config.RedisConfiguration;
import me.jamesfrost.robotsio.RobotsParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RMapCache;
import org.redisson.api.RSetCache;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

/**
 * class for connecting to redis database (LRU cache)
 */
public class RedisConnection {

    private Logger logger = LogManager.getLogger(this.getClass());
    private Config config;
    private RedissonClient redissonClient;
    private RSetCache<Object> domains;
    private int expiredTimeDomainSecond;

    public RedisConnection(RedisConfiguration data) {
        config = new Config();
        config.useSingleServer().setAddress("redis://" + data.getProperty("host.port.1"))
                .setPassword(data.getProperty("redis.password"));
        redissonClient = Redisson.create(config);
        expiredTimeDomainSecond = Integer.parseInt(data.getProperty("expired.time.for.domain.cache"));
        domains = redissonClient.getSetCache(data.getProperty("domains.cache.set"));
        logger.info("redis connection created.");
    }

    public void addDomainInDb(String domain) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        // the result is for logging
        boolean result = domains.add(domain, expiredTimeDomainSecond, TimeUnit.SECONDS);

        if (!result) {
            logger.error("domain add to redis failed. domain : " + domain);
        }
    }

    boolean existsDomainInDB(String key) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        return domains.contains(key);
    }
}
