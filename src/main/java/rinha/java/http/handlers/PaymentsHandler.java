package rinha.java.http.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import rinha.java.worker.ProcessWorker;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Semaphore;

public class PaymentsHandler implements HttpHandler {
    private static final int MAX_BODY_BYTES = 256;
    private static final ThreadLocal<byte[]> TL_BUF = ThreadLocal.withInitial(() -> new byte[MAX_BODY_BYTES]);
    private static final Semaphore SEM = new Semaphore(600, false);

    private final ProcessWorker processWorker = ProcessWorker.getInstance();

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!SEM.tryAcquire()) {
            System.out.println("Crowded with requests, need to wait");
            drainQuiet(exchange);
            reply(exchange, 503);
            return;
        }

        try {
            byte[] data;
            try (InputStream in = exchange.getRequestBody()) {
                data = readFully(in);
            }

            this.processWorker.enqueue(data);
            reply(exchange, 202);
        } finally {
            SEM.release();
        }
    }

    private static byte[] readFully(InputStream in) throws IOException {
        byte[] buf = TL_BUF.get();
        int off = 0;
        int n;
        while ((n = in.read(buf, off, buf.length - off)) != -1) {
            off += n;
            if (off == buf.length) {
                throw new IOException("Payload too large");
            }
        }
        byte[] out = new byte[off];
        System.arraycopy(buf, 0, out, 0, off);
        return out;
    }

    private static void reply(HttpExchange ex, int status) throws IOException {
        ex.sendResponseHeaders(status, 0);
        ex.close();
    }

    private static void drainQuiet(HttpExchange ex) {
        try (InputStream in = ex.getRequestBody()) {
            byte[] tmp = TL_BUF.get();
            //noinspection StatementWithEmptyBody
            while (in.read(tmp) != -1) {
            }
        } catch (IOException ignore) {
        }
    }
}
