package rinha.java.worker;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RedisFlusher {

    private static final byte[] Q_PAYMENTS = "q:payments".getBytes(StandardCharsets.US_ASCII);

    private final JedisPool writePool;
    private final int maxBatch;
    private final long maxWaitNanos;

    private final ConcurrentLinkedQueue<byte[]> q = new ConcurrentLinkedQueue<>();
    private final Semaphore sem = new Semaphore(0);
    private final AtomicBoolean started = new AtomicBoolean(false);

    public RedisFlusher(JedisPool writePool, int maxBatch, Duration maxWait) {
        this.writePool = writePool;
        this.maxBatch = Math.max(1, maxBatch);
        this.maxWaitNanos = Math.max(100_000, maxWait.toNanos()); // >=0.1ms
    }

    public void start() {
        if (!started.compareAndSet(false, true)) return;
        Thread.startVirtualThread(this::run);
        System.out.printf("[RedisFlusher] start batch=%d wait=%dus%n", maxBatch, maxWaitNanos/1_000);
    }

    public void enqueue(byte[] payload) {
        q.offer(payload);
        sem.release();
    }

    private void run() {
        final ArrayList<byte[]> batch = new ArrayList<>(maxBatch);
        try (Jedis jedis = writePool.getResource()) {
            while (true) {
                sem.acquire(); // at least 1
                batch.clear();
                byte[] first = q.poll();
                if (first != null) batch.add(first);

                long deadline = System.nanoTime() + maxWaitNanos;
                while (batch.size() < maxBatch) {
                    byte[] p = q.poll();
                    if (p != null) { batch.add(p); continue; }
                    if (System.nanoTime() >= deadline) break;
                    Thread.onSpinWait();
                }

                long a = System.nanoTime();
                Pipeline p = jedis.pipelined();
                for (byte[] item : batch) p.rpush(Q_PAYMENTS, item);
                p.sync();
                long b = System.nanoTime();

                if ((b-a) >= 10_000_000L) { // 10ms
                    System.out.printf("[RedisFlusher] flushed %d in %dms%n", batch.size(), (b-a)/1_000_000);
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
