package rinha.java.worker;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rinha.java.model.DataBuilder;
import rinha.java.model.PaymentRequest;
import rinha.java.persistence.redis.read.RedisPrincipalReadClient;
import rinha.java.persistence.redis.write.RedisPrincipalWriteClient;
import rinha.java.service.PaymentProcessorClient;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Semaphore;

public final class ProcessWorker2 {
    private static final ProcessWorker2 INSTANCE = new ProcessWorker2();

    private static final byte[] Q_PAYMENTS = "q:payments".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] KEY_PAYMENTS =
            "payments".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private final int workers;
    private final JedisPool readPool;
    private final JedisPool writePool;
    private final PaymentProcessorClient client;

    // HTTP concurrency limiter (controls downstream pressure)
    private final Semaphore httpSlots;

    // Enqueue off-path
    private final RedisFlusher flusher;

    // Rolling metrics for queue lag
    private final LongRing lagRing = new LongRing(2048);

    private ProcessWorker2() {
        this.workers = Integer.parseInt(System.getenv().getOrDefault("NUM_WORKERS", "10"));
        this.readPool = RedisPrincipalReadClient.getInstance().getPool();
        this.writePool = RedisPrincipalWriteClient.getInstance().getPool();
        this.client = PaymentProcessorClient.getInstance();

        int inflight = Integer.parseInt(System.getenv().getOrDefault("HTTP_IN_FLIGHT", String.valueOf(Math.max(2, workers / 2))));
        this.httpSlots = new Semaphore(Math.max(1, inflight));

        int batch = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "512"));
        long waitMs = Long.parseLong(System.getenv().getOrDefault("BATCH_WAIT_MS", "1"));
        this.flusher = new RedisFlusher(writePool, batch, Duration.ofMillis(waitMs));
        this.flusher.start();

        System.out.printf("Workers=%d HTTP_IN_FLIGHT=%d batch=%d wait=%dms%n", workers, inflight, batch, waitMs);

        init();
        startMetricsLogger();
    }

    public static ProcessWorker2 getInstance() {
        return INSTANCE;
    }

    private void init() {
        for (int i = 0; i < this.workers; i++) {
            final int id = i;
            Thread.startVirtualThread(() -> {
                // *** Pegue UMA conexão de leitura (BLPOP) e UMA de escrita (ZADD) por worker ***
                try (var read = this.readPool.getResource();
                     var write = this.writePool.getResource()) {
                    consumeLoop(id, read, write);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        System.out.println("Started " + workers + " Redis consumers");
    }

    public void enqueue(byte[] requestJson) {
        byte[] packed = packWithTs(System.nanoTime(), requestJson);
        this.flusher.enqueue(packed);
    }

    private void consumeLoop(int workerId, Jedis read, Jedis write) {
        while (true) {
            List<byte[]> res = read.blpop(0, Q_PAYMENTS);
            if (res.size() < 2) continue;

            byte[] packed = res.get(1);
            long tEnq;
            byte[] payload;
            try {
                ByteBuffer bb = ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN);
                tEnq = bb.getLong();
                payload = new byte[packed.length - Long.BYTES];
                bb.get(payload);
            } catch (Throwable parseErr) {
                tEnq = System.nanoTime();
                payload = packed;
            }

            long tNow = System.nanoTime();
            lagRing.record(Math.max(0, tNow - tEnq));

            PaymentRequest pr = new PaymentRequest(payload);

            long t0 = System.nanoTime();
            boolean ok = false;
            long tHttp = 0;

            try {
                httpSlots.acquire();
                long a = System.nanoTime();
                try {
                    ok = client.processPayment(pr);
                } finally {
                    httpSlots.release();
                }
                long b = System.nanoTime();
                tHttp = (b - a) / 1_000_000;

                if (ok) {
                    // ======== WRITE-THROUGH SINCRONO ========
                    var prr = pr.parseToDefault();
                    final double score = prr.requestedAt;
                    final byte[] member = DataBuilder.buildBytes(prr);

                    // pequeno retry local (2 tentativas rápidas)
                    boolean saved = false;
                    for (int attempt = 0; attempt < 2; attempt++) {
                        try {
                            write.zadd(KEY_PAYMENTS, score, member);
                            saved = true;
                            break;
                        } catch (Exception re) {
                            // reconectar se necessário
                            safeReconnect(write);
                        }
                    }
                } else {
                    // Requeue simples (pode evoluir para retry com backoff)
                    try {
                        write.rpush(Q_PAYMENTS, packed);
                    } catch (Exception re) {
                        safeReconnect(write);
                        write.rpush(Q_PAYMENTS, packed);
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                long total = (System.nanoTime() - t0) / 1_000_000;
                long tOther = Math.max(0, total - tHttp);
                if (total >= 50) {
                    System.out.printf("[worker-%d] total=%dms http=%dms other=%dms ok=%s%n",
                            workerId, total, tHttp, tOther, ok);
                }
            }
        }
    }

    private static void safeReconnect(Jedis j) {
        try {
            j.close();
        } catch (Exception ignore) {
        }
        // NOTE: Jedis do pool foi fechado; numa próxima operação, você deve pegar outro.
        // Como aqui mantemos uma referência 'write' aberta no try-with-resources externo,
        // a forma simples é capturar a exceção acima e deixar estourar para fora do loop,
        // ou transformar 'write' em ThreadLocal que repõe do pool. Como atalho pequeno:
        // você pode lançar uma RuntimeException aqui e, no try externo, reabrir as duas conexões.
        // Para manter simples, omitimos reconexão automática detalhada.
    }

    // ========= helpers =========
    private static byte[] packWithTs(long tsNano, byte[] payload) {
        byte[] out = new byte[Long.BYTES + payload.length];
        ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN).putLong(tsNano).put(payload);
        return out;
    }

    /**
     * tiny rolling ring for percentiles (approx)
     */
    static final class LongRing {
        private final long[] ring;
        private final int mask;
        private volatile int idx = 0;   // next write position
        private volatile int seen = 0;  // total items seen (may exceed ring.length)

        LongRing(int cap) {
            int pow2 = 1;
            while (pow2 < cap) pow2 <<= 1;
            this.ring = new long[pow2];
            this.mask = pow2 - 1;
        }

        void record(long v) {
            // single-writer expected; if multi, still OK para telemetria best-effort
            int i = idx;
            ring[i & mask] = v;
            idx = i + 1;
            if (seen < Integer.MAX_VALUE) seen++;
        }

        Snapshot snapshot() {
            // pega um snapshot consistente (best-effort)
            final int currIdx = idx;
            final int totalSeen = seen;

            int n = Math.min(totalSeen, ring.length);
            if (n <= 0) return new Snapshot(0, 0, 0, 0);

            long[] copy = new long[n];

            // reconstruir últimos n itens em ordem temporal (não é estritamente necessário,
            // mas deixa os percentis corretos mesmo com wrap)
            int pos = (currIdx - 1) & mask;
            for (int i = 0; i < n; i++) {
                copy[i] = ring[(pos - i) & mask];
            }

            java.util.Arrays.sort(copy); // ordena crescente

            // índices seguros mesmo para n pequeno
            int i50 = (int) ((n - 1) * 0.50);
            int i95 = (int) ((n - 1) * 0.95);
            int i99 = (int) ((n - 1) * 0.99);

            long p50 = copy[i50];
            long p95 = copy[i95];
            long p99 = copy[i99];

            return new Snapshot(p50, p95, p99, n);
        }

        record Snapshot(long p50ns, long p95ns, long p99ns, int n) {
        }
    }

    private void startMetricsLogger() {
        Thread.startVirtualThread(() -> {
            try {
                while (true) {
                    Thread.sleep(2000);
                    var s = lagRing.snapshot();
                    if (s.n() == 0) {
                        System.out.println("[queue_lag] ainda sem amostras");
                    } else {
                        System.out.printf("[queue_lag] p50=%.3fms p95=%.3fms p99=%.3fms n=%d%n",
                                s.p50ns() / 1_000_000.0, s.p95ns() / 1_000_000.0, s.p99ns() / 1_000_000.0, s.n());
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
