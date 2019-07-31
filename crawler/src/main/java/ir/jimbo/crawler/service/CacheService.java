package ir.jimbo.crawler.service;


import com.yammer.metrics.core.HealthCheck;
import ir.jimbo.commons.exceptions.JimboException;
import ir.jimbo.commons.util.HashUtil;
import ir.jimbo.crawler.config.RedisConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/**
 * class for connecting to redis database (LRU cache)
 */
public class CacheService extends HealthCheck {
    private Logger logger = LogManager.getLogger(this.getClass());
    private int expiredTimeDomainMilis;
    private int expiredTimeUrlMilis;
    private HashUtil hashUtil;
    private RedissonClient redis;

    public CacheService(RedisConfiguration redisConfiguration, String redisHealthChecker) {
        super(redisHealthChecker);
        // On closing app
        hashUtil = new HashUtil();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                redis.shutdown();
            } catch (Exception e) { logger.error("exception in closing redisson", e); }
        }));

        Config config = new Config();
        config.useSingleServer().setAddress(redisConfiguration.getNodes().get(0));
        redis = Redisson.create(config);
        expiredTimeDomainMilis = redisConfiguration.getDomainExpiredTime();
        expiredTimeUrlMilis = redisConfiguration.getUrlExpiredTime();
        logger.info("redis connection created.");
    }

    public void addDomain(String domain) {
        if (domain.trim().isEmpty()) {
            return;
        }
        RBucket<Long> bucket = redis.getBucket(domain);
        bucket.set(System.currentTimeMillis());
    }

    public void addUrl(String url) {
        if (url.trim().isEmpty()) {
            return;
        }
        String hashedUri = getMd5(url);
        RBucket<Long> bucket = redis.getBucket(hashedUri);
        bucket.set(System.currentTimeMillis());
    }

    public boolean isDomainExist(String key) {
        if (key.trim().isEmpty()) {
            return false;
        }
        long lastTime;
        try {
            lastTime = (long) redis.getBucket(key).get();
        } catch (Exception e) {
            return false;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTime < expiredTimeDomainMilis) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isUrlExists(String uri) {
        if (uri.trim().isEmpty()) {
            return false;
        }
        String hashedUri = getMd5(uri);
        long lastTime;
        try {
            lastTime = (long) redis.getBucket(hashedUri).get();
        } catch (Exception e) {
            return false;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTime < expiredTimeUrlMilis) {
            return true;
        } else {
            return false;
        }
    }

    private String getMd5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");

            byte[] messageDigest = md.digest(input.getBytes());

            BigInteger no = new BigInteger(1, messageDigest);
            StringBuilder hashText = new StringBuilder(no.toString(16));
            while (hashText.length() < 32) {
                hashText.insert(0, "0");
            }
            return hashText.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new JimboException("fail in creating hash");
        }
    }

    @Override
    protected Result check() {
        if (redis == null)
            return Result.unhealthy("connection is null");
        if (redis.isShutdown())
            return Result.unhealthy("connection is closed");
        if (redis.isShuttingDown())
            return Result.unhealthy("connection is closing");
        return Result.healthy();
    }
}
