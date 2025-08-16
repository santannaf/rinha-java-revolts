package rinha.java.persistence.redis.write;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public final class RedisSecondaryWriteClient {
    private static final RedisSecondaryWriteClient INSTANCE = new RedisSecondaryWriteClient();

    private final JedisPool pool;

    private RedisSecondaryWriteClient() {
        JedisPoolConfig cfg = new JedisPoolConfig();
        var host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        cfg.setMaxTotal(2);
        cfg.setMaxIdle(2);
        cfg.setMinIdle(2);
        cfg.setBlockWhenExhausted(true);
        cfg.setJmxEnabled(false);
        cfg.setTestOnBorrow(false);
        cfg.setTestOnReturn(false);
        cfg.setTestWhileIdle(false);

        this.pool = new JedisPool(cfg, host, 6379, 200);
    }

    public static RedisSecondaryWriteClient getInstance() {
        return INSTANCE;
    }

    public JedisPool getPool() {
        return pool;
    }
}
