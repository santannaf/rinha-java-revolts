package rinha.java.http;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class HttpClientConfig {
    private static final HttpClientConfig INSTANCE = new HttpClientConfig();

    private final HttpClient httpClient;

    private HttpClientConfig() {
        final boolean useVirtual = Boolean.parseBoolean(getenv("HTTP_VIRTUAL", "true"));
        final int core = Integer.parseInt(getenv("HTTP_CORE_THREADS", "4"));
        final int max = Integer.parseInt(getenv("HTTP_MAX_THREADS", "16"));
        final int qcap = Integer.parseInt(getenv("HTTP_QUEUE", "256"));
        final long keepAliveSec = Long.parseLong(getenv("HTTP_KEEPALIVE_SEC", "30"));
        final long connectTimeoutMs = Long.parseLong(getenv("HTTP_CONNECT_TIMEOUT_MS", "500"));

        // ---- thread factory (virtual por padrão) ----
        final ThreadFactory tf = useVirtual
                ? Thread.ofVirtual().name("http-vt-", 0).factory()
                : new ThreadFactory() {
            final AtomicInteger n = new AtomicInteger();

            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "http-pt-" + n.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };

        // ---- fila limitada + CallerRunsPolicy (backpressure) ----
        final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(qcap);

        // quando cheio: executa no chamador
        ExecutorService executor = new ThreadPoolExecutor(
                core, max, keepAliveSec, TimeUnit.SECONDS,
                queue, tf,
                new ThreadPoolExecutor.CallerRunsPolicy() // quando cheio: executa no chamador
        );

        this.httpClient = HttpClient.newBuilder()
                .executor(executor)
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                .followRedirects(HttpClient.Redirect.NEVER)
                // dica: mude para HTTP_1_1 se o backend não falar HTTP/2
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    public static HttpClientConfig getInstance() {
        return INSTANCE;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    private static String getenv(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v;
    }
}
