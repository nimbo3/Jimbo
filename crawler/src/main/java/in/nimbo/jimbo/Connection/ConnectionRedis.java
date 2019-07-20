package in.nimbo.jimbo.Connection;

import org.redisson.Redisson;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

/**
 * class for connecting to redis database (LRU cache)
 */
public class ConnectionRedis {

    Config config;
    RedissonClient redissonClient;
    RMapCache<String, String> urls;
    RMapCache<String, Object> robots;    /////////////////////////////////////////

    public ConnectionRedis(String hostPort1, String password1, String hostPort2) {

        config = new Config();
        config.useReplicatedServers()
                .addNodeAddress("redis://" + hostPort1, "redis://" + hostPort2).setPassword(password1);
        redissonClient = Redisson.create(config);
        urls = redissonClient.getMapCache("urls");
        robots = redissonClient.getMapCache("robots");
    }

    void addLinkToDb(String key, String value, int expiredTimeSecond) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        // the result is for logging
        boolean result = urls.fastPut(key, value, expiredTimeSecond, TimeUnit.SECONDS);

        if (!result) {
            //
        }
    }

    void addRobotsToDb(String hostAsKey, Object robot, int expiredTimeInHour) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        // the result is for logging
        boolean result = robots.fastPut(hostAsKey, robot, expiredTimeInHour, TimeUnit.HOURS);
        if (!result) {
            //
        }
    }

    boolean existsLinkInDB(String key) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        return urls.containsKey(key);
    }

    boolean existRobotsInDb(String hostAsKey) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        return robots.containsKey(hostAsKey);
    }

    Object getRobotsOfDb(String hostAsKey) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        return robots.get(hostAsKey);
    }

}
