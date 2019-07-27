package ir.jimbo.crawler.service;


import ir.jimbo.crawler.config.RedisConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;


/**
 * class for connecting to redis database (LRU cache)
 */
public class CacheService {
    private Logger logger = LogManager.getLogger(this.getClass());
    private int expiredTimeDomainMilis;
    private int expiredTimeUrlMilis;
    private Jedis jedis;

    public CacheService(RedisConfiguration redisConfiguration) {

        // On closing app
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                jedis.disconnect();
            } catch (Exception e) {
                logger.error("exception in closing jedis", e);
            }
        }));
        jedis = new Jedis();
        expiredTimeDomainMilis = redisConfiguration.getDomainExpiredTime();
//        expiredTimeUrlMilis =
        logger.info("redis connection created.");
    }

    public void addDomain(String domain) {
        jedis.set(domain, String.valueOf(System.currentTimeMillis()));
    }

    public void addUrl(String url) {
        jedis.set(url, String.valueOf(System.currentTimeMillis()));
    }

    public boolean isDomainExist(String key) {
        long lastTime;
        try {
            lastTime = Long.parseLong(jedis.get(key));
        } catch (Exception e) {
            return true;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTime < expiredTimeDomainMilis) {
            return true;
        } else {
            jedis.del(key);
            return false;
        }
    }

    public boolean isUrlExists(String url) {
        long lastTime;
        try {
            lastTime = Long.parseLong(jedis.get(url));
        } catch (Exception e) {
            return true;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTime < expiredTimeUrlMilis) {
            return true;
        } else {
            jedis.del(url);
            return false;
        }
    }
}
