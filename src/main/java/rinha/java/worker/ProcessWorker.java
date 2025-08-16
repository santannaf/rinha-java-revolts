package rinha.java.worker;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rinha.java.model.DataBuilder;
import rinha.java.model.PaymentRequest;
import rinha.java.persistence.redis.read.RedisPrincipalReadClient;
import rinha.java.persistence.redis.write.RedisPrincipalWriteClient;
import rinha.java.service.PaymentProcessorClient;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public final class ProcessWorker {
    private final static ProcessWorker INSTANCE = new ProcessWorker();
    private final Integer workers;
    private final JedisPool readPool;
    private final JedisPool writePool;
    private final ConcurrentLinkedQueue<PaymentRequest> queue = new ConcurrentLinkedQueue<>();
    private final Semaphore semaphore = new Semaphore(0);

    private static final byte[] KEY_PAYMENTS = "payments".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private final PaymentProcessorClient client;

    private ProcessWorker() {
        this.workers = Integer.parseInt(System.getenv().getOrDefault("NUM_WORKERS", "10"));
        this.writePool = RedisPrincipalWriteClient.getInstance().getPool();
        this.readPool = RedisPrincipalReadClient.getInstance().getPool();
        this.client = PaymentProcessorClient.getInstance();
        System.out.println("Worker count: " + workers);
        Thread.startVirtualThread(this::init);
    }

    public static ProcessWorker getInstance() {
        return INSTANCE;
    }

    public void init() {
        for (int i = 0; i < this.workers; i++) {
            Thread.startVirtualThread(() -> {
                try (Jedis write = this.writePool.getResource(); Jedis read = this.readPool.getResource()) {
                    start(read, write);
                } catch (Exception _) {
                }
            });
        }
    }

    private void start(Jedis read, Jedis write) {
        for (; ; ) {
            try {
                if (Boolean.parseBoolean(read.get("health-payment"))) {
                    semaphore.acquire();
                    PaymentRequest pr = queue.poll();
                    if (pr == null) { // corrida entre workers
                        semaphore.release();
                        continue;
                    }

                    processPayment(write, pr);
                }

            } catch (Exception _) {
            }
        }
    }

    private void processPayment(Jedis write, PaymentRequest pr) {
        if (client.processPayment(pr)) {
            var prr = pr.parseToDefault();
            final double score = prr.requestedAt;
            final byte[] member = DataBuilder.buildBytes(prr);
            write.zadd(KEY_PAYMENTS, score, member);
        }


        else {
            if (client.sendPaymentFallback(pr)) {
                var prr = pr.parseToFallback();
                final double score = prr.requestedAt;
                final byte[] member = DataBuilder.buildBytes(prr);
                write.zadd(KEY_PAYMENTS, score, member);
                return;
            }

            queue.offer(pr);
            semaphore.release();
            write.set("health-payment", "false");
        }
    }

    public void enqueue(byte[] request) {
        queue.offer(new PaymentRequest(request));
        semaphore.release();
    }
}
