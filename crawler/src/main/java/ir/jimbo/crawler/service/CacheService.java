package ir.jimbo.crawler.service;


import ir.jimbo.crawler.config.RedisConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;

/**
 * class for connecting to redis database (LRU cache)
 */
public class CacheService {

    private Logger logger = LogManager.getLogger(this.getClass());
    private RedissonClient redissonClient;
    private int expiredTimeDomainMilis;

    public CacheService(RedisConfiguration redisConfiguration) {

        // On closing app
        Runtime.getRuntime().addShutdownHook(new Thread(() -> redissonClient.shutdown()));

        Config config = new Config();
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
        expiredTimeDomainMilis = redisConfiguration.getExpiredTime();
        logger.info("redis connection created.");
    }

    public void addDomain(String domain) {
        RBucket<Object> bucket = redissonClient.getBucket(domain);
        long timeMillis = System.currentTimeMillis();
        bucket.set(timeMillis);
        logger.info("a domain added");
    }

    public boolean isDomainExist(String key) {
        RBucket<Object> bucket = redissonClient.getBucket(key);
        Long lastTime = (Long) bucket.get();
        if (lastTime == null)
            return false;
        long currentTime = System.currentTimeMillis();
        logger.info("checking politeness. current time : " + currentTime + " lastTime : " + lastTime);
        return currentTime - lastTime < expiredTimeDomainMilis;
    }
}
