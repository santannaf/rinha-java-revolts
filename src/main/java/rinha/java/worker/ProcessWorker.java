package rinha.java.worker;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZAddParams;
import rinha.java.model.DataBuilder;
import rinha.java.model.PaymentRequest;
import rinha.java.persistence.redis.RedisPool;
import rinha.java.service.PaymentProcessorClient;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

public final class ProcessWorker {
    private static final ProcessWorker INSTANCE = new ProcessWorker();

    private static final byte[] Q_PAYMENTS = "q:payments".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] KEY_PAYMENTS = "payments".getBytes(StandardCharsets.US_ASCII);

    private final RedisFlusher flusher;
    private final int workers = Integer.parseInt(System.getenv().getOrDefault("NUM_WORKERS", "16"));
    private final PaymentProcessorClient client = PaymentProcessorClient.getInstance();

    private ProcessWorker() {
        int inflight = Integer.parseInt(System.getenv().getOrDefault("HTTP_IN_FLIGHT", String.valueOf(Math.max(2, workers / 2))));
        int batch = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "512"));
        long waitMs = Long.parseLong(System.getenv().getOrDefault("BATCH_WAIT_MS", "0"));

        this.flusher = new RedisFlusher(RedisPool.getInstance().getPool(), batch, Duration.ofMillis(waitMs));
        this.flusher.start();

        System.out.printf("Workers=%d HTTP_IN_FLIGHT=%d batch=%d wait=%dms%n", workers, inflight, batch, waitMs);
        init();
    }

    public static ProcessWorker getInstance() {
        return INSTANCE;
    }

    private void init() {
        for (int i = 0; i < this.workers; i++) {
            Thread.startVirtualThread(() -> {
                try (var jedis = RedisPool.getInstance().getPool().getResource(); var exclusive = RedisPool.getInstance().getPool().getResource()) {
                    consumeLoop(jedis, exclusive);
                } catch (Exception ignored) {
                }
            });
        }

        System.out.println("Started " + workers + " Redis consumers");
    }

    public void enqueue(byte[] requestJson) {
        byte[] packed = packWithTs(System.nanoTime(), requestJson);
        this.flusher.enqueue(packed);
    }

    private void saveZaa(Jedis exclusive, PaymentRequest prr) {
        final double score = prr.requestedAt;
        final byte[] member = DataBuilder.buildBytes(prr);

        try {
            long t0 = System.nanoTime();
            ZAddParams nx = ZAddParams.zAddParams().nx();
            long added = exclusive.zadd(KEY_PAYMENTS, score, member, nx);
            long borrowUs = (System.nanoTime() - t0) / 1000;
            if (borrowUs > 20000) System.out.printf("[worker-process] borrow zadd(...) took %d µs%n", borrowUs);

            if (added == 0L) {
                if ((System.nanoTime() & 0x3FF) == 0) {
                    System.out.println("[warn] ZADD NX returned 0 (duplicate member?)");
                }
            }
        } catch (Exception re) {
            System.err.println(re.getMessage());
            safeReconnect(exclusive);
        }
    }

    private void consumeLoop(Jedis jedis, Jedis exclusive) {
        for (; ; ) {
            List<byte[]> res = jedis.blpop(0, Q_PAYMENTS);
            if (res.size() < 2) continue;

            byte[] packed = res.get(1);

            final PaymentRequest pr = new PaymentRequest(packed);
            final byte[] p = pr.getPostData();

            if (client.processPayment(p)) {
                var prr = pr.parseToDefault();
                saveZaa(exclusive, prr);
            } else {
                try {
                    jedis.rpush(Q_PAYMENTS, packed);
                } catch (Exception re) {
                    safeReconnect(jedis);
                    jedis.rpush(Q_PAYMENTS, packed);
                }
            }
        }
    }

    private static void safeReconnect(Jedis j) {
        try {
            j.close();
        } catch (Exception ignore) {
        }
    }

    private static byte[] packWithTs(long tsNano, byte[] payload) {
        byte[] out = new byte[Long.BYTES + payload.length];
        ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN).putLong(tsNano).put(payload);
        return out;
    }
}
