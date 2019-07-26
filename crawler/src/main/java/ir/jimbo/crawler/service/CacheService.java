package ir.jimbo.crawler.service;


import ir.jimbo.crawler.config.RedisConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * class for connecting to redis database (LRU cache)
 */
public class CacheService {


//    private Logger logger = LogManager.getLogger(this.getClass());
    private int expiredTimeDomainMilis;
//    private Jedis jedis;
    private Map<String, Long> domains;

    public CacheService(RedisConfiguration redisConfiguration) {

        // On closing app
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                jedis.disconnect();
//                jedis.close();
//            } catch (Exception e) {
//                logger.error("exception in closing jedis", e);
//            }
//        }));
//        jedis = new Jedis();
//        jedis.connect();
        expiredTimeDomainMilis = redisConfiguration.getExpiredTime();
//        logger.info("redis connection created.");
        domains = new HashMap<>();
    }

    public void addDomain(String domain) {
//        jedis.set(domain, String.valueOf(System.currentTimeMillis()));
        domains.put(domain, System.currentTimeMillis());
    }

    public boolean isDomainExist(String key) {
        long lastTime;
        try {
            lastTime = domains.get(key);
        } catch (Exception e) {
            return true;
        }
        long currentTime = System.currentTimeMillis();
        return currentTime - lastTime < expiredTimeDomainMilis;
    }
}
