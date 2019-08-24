package ir.jimbo.train.data.service;


import ir.jimbo.train.data.config.RedisConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * class for connecting to redis database (LRU cache)
 */
public class CacheService {
    private Logger logger = LogManager.getLogger(this.getClass());
    private int politenessTime;
    private RedissonClient redis;

    public CacheService(RedisConfiguration redisConfiguration, int politenessTime) {
        // On closing app
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                redis.shutdown();
            } catch (Exception e) {
                logger.error("exception in closing redisson", e);
            }
        }));
        this.politenessTime = politenessTime;
        Config config = new Config();
        if (!redisConfiguration.getPassword().isEmpty()) {
            config.useSingleServer().setAddress(redisConfiguration.getNodes().get(0)).setPassword(redisConfiguration.getPassword());
        } else {
            config.useSingleServer().setAddress(redisConfiguration.getNodes().get(0));
        }
        redis = Redisson.create(config);
        logger.info("redis connection created.");
        if (redis == null || redis.isShutdown()) {
            logger.error("redisson in not connected to redis. start shutting down app");
            System.exit(0);
        }
    }

    public void addDomain(String domain) {
        if (domain.trim().isEmpty()) {
            return;
        }
        RBucket<Long> bucket = redis.getBucket(domain);
        bucket.set(System.currentTimeMillis());
    }

    public boolean isDomainExist(String key) {
        if (key.trim().isEmpty()) {
            return false;
        }
        long lastTime;
        try {
            lastTime = (long) redis.getBucket(key).get();
        } catch (NullPointerException e) {
            return false;
        } catch (Exception e) {
            return true;
        }
        long currentTime = System.currentTimeMillis();
        return currentTime - lastTime < politenessTime;
    }

    public void close() {
        redis.shutdown();
    }
}
