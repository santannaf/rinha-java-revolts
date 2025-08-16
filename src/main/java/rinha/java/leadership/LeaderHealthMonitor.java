package rinha.java.leadership;

import redis.clients.jedis.Jedis;
import rinha.java.http.HttpClientConfig;
import rinha.java.persistence.redis.write.RedisSecondaryWriteClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class LeaderHealthMonitor {
    private static final LeaderHealthMonitor INSTANCE = new LeaderHealthMonitor();
    private static final RedisSecondaryWriteClient redisWrite = RedisSecondaryWriteClient.getInstance();
    private static final long PERIOD = 5;

    private final ScheduledExecutorService scheduler;
    private final URI defaultURI;
    private final HttpClient httpClient;

    private LeaderHealthMonitor() {
        this.scheduler = Executors.newScheduledThreadPool(
                1,
                Thread.ofVirtual().name("leader-health-", 0).factory()
        );

        var defaultURL = System.getenv().getOrDefault("HOST_PROCESSOR_DEFAULT", "http://localhost:8001");
        defaultURI = URI.create(defaultURL.concat("/payments/service-health"));
        this.httpClient = HttpClientConfig.getInstance().getHttpClient();
    }

    public static LeaderHealthMonitor getInstance() {
        return INSTANCE;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::probeOnce, 0, PERIOD, TimeUnit.SECONDS);
    }

    private void probeOnce() {
        boolean available;
        try {
            var request = HttpRequest.newBuilder(defaultURI).GET().build();
            var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            boolean failing = extractFailing(response.body());
            available = !failing;
        } catch (Exception ex) {
            available = false;
        }

        System.out.println("Health check monitor: " + available);

        try {
            try (Jedis jedis = redisWrite.getPool().getResource()) {
                jedis.set("health-payment", Boolean.toString(available));
            }
        } catch (Exception ignored) {
        }
    }

    private static boolean extractFailing(String body) {
        if (body == null || body.isEmpty()) return false;

        int idx = body.indexOf("\"failing\"");
        if (idx == -1) return false;

        int colon = body.indexOf(':', idx);
        if (colon == -1) return false;

        String value = body.substring(colon + 1).trim();

        return value.startsWith("true");
    }
}
