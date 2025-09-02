package rinha.java.http.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import rinha.java.model.PaymentSummary;
import rinha.java.persistence.redis.RedisPool;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

public class PaymentsSummaryHandler implements HttpHandler {
    private static final RedisPool redisSecondaryRead = RedisPool.getInstance();

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        final URI uri = exchange.getRequestURI();
        final String q = uri.getRawQuery();
        final String fromStr = getQuery(q, "from");
        final String toStr = getQuery(q, "to");

        final Instant now = Instant.now();
        Instant from;
        Instant to;

        if (fromStr == null || fromStr.isBlank()) {
            // intervalo mínimo absoluto
            from = Instant.ofEpochMilli(Long.MIN_VALUE);
        } else {
            from = parseInstantSafe(fromStr);
        }

        if (toStr == null || toStr.isBlank()) {
            to = now;
        } else {
            to = parseInstantSafe(toStr);
        }

        // valida parsing e ordem do intervalo
        if (from == null || to == null || to.isBefore(from)) {
            exchange.sendResponseHeaders(400, 0);
            exchange.close();
            return;
        }

        final long fromMs = from.toEpochMilli();
        final long toMs = to.toEpochMilli();

        var s = range(fromMs, toMs);

        final byte[] out = s.toJson().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, out.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(out);
        }
    }

    private static final JedisPool pool = RedisPool.getInstance().getPool();

    private PaymentSummary range(long fromMs, long toMs) {
        try (Jedis read = pool.getResource()) {
            List<String> defBuckets = read.zrangeByScore("payments", fromMs, toMs);

            long defaultCount = 0;
            long fallbackCount = 0;
            long defaultAmount = 0L;
            long fallbackAmount = 0L;

            for (String payment : defBuckets) {
                String[] parts = payment.split(":");
                long amount = Long.parseLong(parts[1]);
                boolean isDefault = parts[2].equals("1");

                if (isDefault) {
                    defaultCount++;
                    defaultAmount += amount;
                } else {
                    fallbackCount++;
                    fallbackAmount += amount;
                }
            }

            return new PaymentSummary(
                    new PaymentSummary.Summary(defaultCount, new BigDecimal(defaultAmount).movePointLeft(2)),
                    new PaymentSummary.Summary(fallbackCount, new BigDecimal(fallbackAmount).movePointLeft(2))
            );
        }
    }

    private static Instant parseInstantSafe(String s) {
        try {
            return Instant.parse(s);
        } catch (Exception e) {
            return null;
        }
    }

    private static String getQuery(String raw, String key) {
        if (raw == null) return null;
        int p = raw.indexOf(key + "=");
        if (p < 0) return null;
        int s = p + key.length() + 1, e = raw.indexOf('&', s);
        String v = (e < 0) ? raw.substring(s) : raw.substring(s, e);
        return java.net.URLDecoder.decode(v, java.nio.charset.StandardCharsets.UTF_8);
    }
}
