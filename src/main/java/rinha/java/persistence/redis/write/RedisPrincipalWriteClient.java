package rinha.java.persistence.redis.write;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public final class RedisPrincipalWriteClient {
    private static final RedisPrincipalWriteClient INSTANCE = new RedisPrincipalWriteClient();

    private final JedisPool pool;

    private RedisPrincipalWriteClient() {
        JedisPoolConfig cfg = new JedisPoolConfig();
        var host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int n = Integer.parseInt(System.getenv().getOrDefault("NUM_WORKERS_WRITE", "10"));

        cfg.setMaxTotal(n);
        cfg.setMaxIdle(n);
        cfg.setMinIdle(n);
        cfg.setBlockWhenExhausted(true);
        cfg.setJmxEnabled(false);
        cfg.setTestOnBorrow(false);
        cfg.setTestOnReturn(false);
        cfg.setTestWhileIdle(false);

        this.pool = new JedisPool(cfg, host, 6379, 200);
    }

    public static RedisPrincipalWriteClient getInstance() {
        return INSTANCE;
    }

    public JedisPool getPool() {
        return pool;
    }
}
