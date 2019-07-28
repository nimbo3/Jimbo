package ir.jimbo.crawler.service;


import ir.jimbo.commons.exceptions.JimboException;
import ir.jimbo.crawler.config.RedisConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


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
        jedis = new Jedis(HostAndPort.parseString(redisConfiguration.getNodes().get(0)));
        expiredTimeDomainMilis = redisConfiguration.getDomainExpiredTime();
        expiredTimeUrlMilis = redisConfiguration.getUrlExpiredTime();
        logger.info("redis connection created.");
    }

    public void addDomain(String domain) {
        jedis.set(domain, String.valueOf(System.currentTimeMillis()));
    }

    public void addUrl(String url) {
        String hashedUri = getMd5(url);
        jedis.set(hashedUri, String.valueOf(System.currentTimeMillis()));
    }

    public boolean isDomainExist(String key) {
        long lastTime;
        try {
            lastTime = Long.parseLong(jedis.get(key));
        } catch (Exception e) {
            return false;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTime < expiredTimeDomainMilis) {
            return true;
        } else {
            jedis.del(key);
            return false;
        }
    }

    public boolean isUrlExists(String uri) {
        String hashedUri = getMd5(uri);
        long lastTime;
        try {
            lastTime = Long.parseLong(jedis.get(hashedUri));
        } catch (Exception e) {
            return false;
        }
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastTime < expiredTimeUrlMilis) {
            return true;
        } else {
            jedis.del(hashedUri);
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
}
