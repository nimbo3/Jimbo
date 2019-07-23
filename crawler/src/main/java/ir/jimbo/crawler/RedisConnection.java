package ir.jimbo.crawler;

import com.panforge.robotstxt.RobotsTxt;
import ir.jimbo.crawler.config.RedisConfiguration;
import org.redisson.Redisson;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

/**
 * class for connecting to redis database (LRU cache)
 */
public class RedisConnection {

    private Config config;
    private RedissonClient redissonClient;
    private RMapCache<String, String> urls;
    private RMapCache<String, RobotsTxt> robots;
    private int expiredTimeDomainSecond;
    private int expiredTimerobotsHour;

    public RedisConnection(RedisConfiguration data) {
        config = new Config();
        config.useSingleServer().setAddress("redis://" + data.getProperty("host.port.1"))
                .setPassword(data.getProperty("redis.password"));

//        config.useReplicatedServers().addNodeAddress("redis://" + data.getProperty("host.port.1"),
//                "redis://" + data.getProperty("host.port.2")).setPassword(data.getProperty("redis.password"));

        redissonClient = Redisson.create(config);
        urls = redissonClient.getMapCache(data.getProperty("urls.cache.map"));
        robots = redissonClient.getMapCache(data.getProperty("robots.cache.map"));
        expiredTimeDomainSecond = Integer.parseInt(data.getProperty("expired.time.for.domain.cache"));
        expiredTimerobotsHour = Integer.parseInt(data.getProperty("expired.time.for.robots.cache"));
    }

    void addDomainInDb(String key, String value) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        // the result is for logging
        boolean result = urls.fastPut(key, value, expiredTimeDomainSecond, TimeUnit.SECONDS);

        if (!result) {
            //
        }
    }

    void addRobotToDB(String hostAsKey, RobotsTxt robot) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        // the result is for logging
        boolean result = robots.fastPut(hostAsKey, robot, expiredTimerobotsHour, TimeUnit.HOURS);

        if (!result) {
            //
        }
    }

    boolean existsDomainInDB(String key) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        return urls.containsKey(key);
    }

    boolean existsRobotInDB(String hostAsKey) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        return robots.containsKey(hostAsKey);
    }

    RobotsTxt getRobotsOfDB(String hostAsKey) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        return robots.get(hostAsKey);
    }
}
