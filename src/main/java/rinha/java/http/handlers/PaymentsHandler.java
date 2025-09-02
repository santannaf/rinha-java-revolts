package rinha.java.http.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import rinha.java.worker.ProcessWorker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class PaymentsHandler implements HttpHandler {
    private static final int MAX_BODY_BYTES = 256;
    private static final ThreadLocal<byte[]> TL_BUF = ThreadLocal.withInitial(() -> new byte[MAX_BODY_BYTES]);

    private final ProcessWorker processWorker = ProcessWorker.getInstance();

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().set("Connection", "keep-alive");
        exchange.sendResponseHeaders(202, 1); // Content-Length: 1
        var out = exchange.getResponseBody();
        out.write('\n');
        out.flush();

        byte[] data;
        try (InputStream in = exchange.getRequestBody()) {
            data = readFully(in);
        }

        long ingressEpochMs = System.currentTimeMillis();
        byte[] packed = new byte[8 + data.length];
       ByteBuffer.wrap(packed).putLong(ingressEpochMs).put(data);

        this.processWorker.enqueue(packed);

        out.close();
        exchange.close();
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
}
