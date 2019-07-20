package in.nimbo.jimbo.Connection;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * class for connecting to redis database (LRU cache)
 */
public class ConnectionRedis {

    private RedissonClient redissonClient;

    public ConnectionRedis(String hostPort1, String password1, String hostPort2) {

        Config config = new Config();
        config.useReplicatedServers()
                .addNodeAddress("redis://" + hostPort1, "redis://" + hostPort2).setPassword(password1);
        redissonClient = Redisson.create(config);
    }

    void addToDb(String key, String value, int expiredTimeSecond) {
    }

    boolean existsInDB(String key) {
        return false;
    }

}
