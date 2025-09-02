package rinha.java.persistence.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.ArrayList;

public final class RedisPool {
    private static final RedisPool INSTANCE = new RedisPool();
    private static final JedisPool POOL;

    static {
        JedisPoolConfig cfg = new JedisPoolConfig();

        int httpEnq = Integer.parseInt(System.getenv().getOrDefault("HTTP_ENQ_CONN","64"));
        int workers = Integer.parseInt(System.getenv().getOrDefault("NUM_WORKERS", "16"));
        int maxTotal = workers * 2 + httpEnq;

        cfg.setMaxTotal(maxTotal);
        cfg.setMaxIdle(maxTotal);
        cfg.setBlockWhenExhausted(true);
        cfg.setJmxEnabled(false);
        cfg.setTestOnBorrow(false);
        cfg.setTestOnReturn(false);
        cfg.setTestWhileIdle(false);
        cfg.setMaxWait(Duration.ofMillis(1));

        String host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int timeoutMs = Integer.parseInt(System.getenv().getOrDefault("REDIS_TIMEOUT_MS", "1000"));

        POOL = new JedisPool(cfg, host, 6379, timeoutMs);

        try {
            ArrayList<Jedis> warm = new ArrayList<>(maxTotal);
            for (int i = 0; i < maxTotal; i++) warm.add(POOL.getResource());
            for (Jedis j : warm) j.close();
        } catch (Exception ignore) {}

        System.out.printf("[RedisPool] host=%s timeout=%dms maxTotal=%d%n", host, timeoutMs, maxTotal);
    }

    private RedisPool() {
    }

    public static RedisPool getInstance() {
        return INSTANCE;
    }

    public JedisPool getPool() {
        return POOL;
    }
}
