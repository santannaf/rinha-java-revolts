package rinha.java.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import rinha.java.http.handlers.PaymentsHandler;
import rinha.java.http.handlers.PaymentsSummaryHandler;

import java.io.IOException;

public final class PathHandler {
    private static final PathHandler INSTANCE = new PathHandler();

    private final HttpHandler handlers;

    private PathHandler() {
        final HttpHandler payments = new PaymentsHandler();
        final HttpHandler paymentsSummary = new PaymentsSummaryHandler();
        this.handlers = new Router(payments, paymentsSummary);
    }

    public HttpHandler getHandlers() {
        return this.handlers;
    }

    public static PathHandler getInstance() {
        return INSTANCE;
    }

    private record Router(HttpHandler payments, HttpHandler paymentsSummary) implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String path = exchange.getRequestURI().getRawPath();
            switch (path) {
                case "/payments":
                    payments.handle(exchange);
                    return;
                case "/payments-summary":
                    paymentsSummary.handle(exchange);
                    return;
                default:
                    exchange.sendResponseHeaders(404, 0);
                    exchange.close();
            }
        }
    }
}
