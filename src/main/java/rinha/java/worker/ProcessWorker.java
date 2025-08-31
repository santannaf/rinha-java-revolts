/*package rinha.java.worker;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rinha.java.model.DataBuilder;
import rinha.java.model.PaymentRequest;
import rinha.java.persistence.redis.read.RedisPrincipalReadClient;
import rinha.java.persistence.redis.write.RedisPrincipalWriteClient;
import rinha.java.service.PaymentProcessorClient;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class ProcessWorker {
    private final static ProcessWorker INSTANCE = new ProcessWorker();
    private final Integer workers;
    private final JedisPool readPool;
    private final JedisPool writePool;
    private final ConcurrentLinkedQueue<PaymentRequest> queue = new ConcurrentLinkedQueue<>();
    private final Semaphore semaphore = new Semaphore(0);

    private static final byte[] KEY_PAYMENTS = "payments".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private final PaymentProcessorClient client;

    private final AtomicInteger activeWorkers = new AtomicInteger(0); // <- NEW
    private final AtomicBoolean failingFlag = new AtomicBoolean(true);

    private ProcessWorker() {
        this.workers = Integer.parseInt(System.getenv().getOrDefault("NUM_WORKERS", "10"));
        this.writePool = RedisPrincipalWriteClient.getInstance().getPool();
        this.readPool = RedisPrincipalReadClient.getInstance().getPool();
        this.client = PaymentProcessorClient.getInstance();
        System.out.println("Worker count: " + workers);
        //init();
    }

    public static ProcessWorker getInstance() {
        return INSTANCE;
    }

    //public void init() {
    //    for (int i = 0; i < this.workers; i++) {
    //        Thread.startVirtualThread(() -> {
    //            try (Jedis write = this.writePool.getResource(); Jedis read = this.readPool.getResource()) {
    //                start(read, write);
    //            } catch (Exception _) {
    //            }
    //        });
    //    }
    //}
    public void init() {
        for (int i = 0; i < this.workers; i++) {
            final int id = i;
            Thread.startVirtualThread(() -> {
                try (var read = this.readPool.getResource()) {
                    consumeLoop(id, read);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        System.out.println("Started " + workers + " Redis consumers");
    }

    private boolean processPayment(PaymentRequest pr) {
        try(var write = writePool.getResource()) {
            if (client.processPayment(pr)) {
                var prr = pr.parseToDefault();
                final double score = prr.requestedAt;
                final byte[] member = DataBuilder.buildBytes(prr);
                write.zadd(KEY_PAYMENTS, score, member);
                return true;
            }

            if (client.sendPaymentFallback(pr)) {
                var prr = pr.parseToFallback();
                final double score = prr.requestedAt;
                final byte[] member = DataBuilder.buildBytes(prr);
                write.zadd(KEY_PAYMENTS, score, member);
                return true;
            }

            write.rpush(Q_PAYMENTS, pr.requestData);
            semaphore.release();
            return false;
        }

        //if (client.sendPaymentFallback(pr)) {
        //    var prr = pr.parseToFallback();
        //    final double score = prr.requestedAt;
        //    final byte[] member = DataBuilder.buildBytes(prr);
        //    write.zadd(KEY_PAYMENTS, score, member);
        //    return;
        //}
        //queue.offer(pr);
        //semaphore.release();
        //write.set("health-payment", "false");
    }

    private static final byte[] Q_PAYMENTS = "q:payments".getBytes(StandardCharsets.US_ASCII);

    public void enqueue(byte[] request) {
        //var payment = new PaymentRequest(request);
        //queue.offer(payment);
        //semaphore.release();

        try (var jedis = writePool.getResource()) {
            jedis.rpush(Q_PAYMENTS, request);
        }
    }

    // NOVO CODE

    private void consumeLoop(int workerId, Jedis read) {
        for (; ; ) {
            List<byte[]> res = read.blpop(0, Q_PAYMENTS);
            if (res.size() < 2) continue;

            byte[] payload = res.get(1);
            PaymentRequest pr = new PaymentRequest(payload);

            long t0 = System.nanoTime();
            boolean ok = false;
            long tHttp=0, tRedis=0, tOther=0;

            try {
                long a = System.nanoTime();
                ok = client.processPayment(pr);          // suspeito: deve estar levando ~1000ms
                long b = System.nanoTime();
                tHttp = (b - a)/1_000_000;

                if (ok) {
                    a = System.nanoTime();
                    var prr = pr.parseToDefault();
                    final double score = prr.requestedAt;
                    final byte[] member = DataBuilder.buildBytes(prr);
                    try (var w = writePool.getResource()) {
                        //w.zadd(KEY_PAYMENTS, score, member);
                    }
                    b = System.nanoTime();
                    tRedis = (b - a)/1_000_000;
                } else {
                    try (var w = writePool.getResource()) { w.rpush(Q_PAYMENTS, payload); }
                }
            } finally {
                long total = (System.nanoTime() - t0)/1_000_000;
                tOther = Math.max(0, total - tHttp - tRedis);
                if (total >= 50) {
                    System.out.printf("[worker-%d] total=%dms http=%dms redis=%dms other=%dms ok=%s%n",
                            workerId, total, tHttp, tRedis, tOther, ok);
                }
            }
        }
    }
}
*/