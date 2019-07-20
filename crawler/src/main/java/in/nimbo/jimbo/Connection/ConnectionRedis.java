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

    RedissonClient redissonClient;
    RMapCache<String, String> urls;
    Config config;

    public ConnectionRedis(String hostPort1, String password1, String hostPort2) {

        config = new Config();
        config.useReplicatedServers()
                .addNodeAddress("redis://" + hostPort1, "redis://" + hostPort2).setPassword(password1);
        redissonClient = Redisson.create(config);
        urls = redissonClient.getMapCache("urls");
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

    boolean existsInDB(String key) {
        if (redissonClient.isShutdown()) {
            redissonClient = Redisson.create(config);
        }
        return urls.containsKey(key);
    }

}
