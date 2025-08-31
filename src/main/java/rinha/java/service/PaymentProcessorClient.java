package rinha.java.service;

import rinha.java.http.HttpClientConfig;
import rinha.java.model.PaymentRequest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

public final class PaymentProcessorClient {
    private static final PaymentProcessorClient INSTANCE = new PaymentProcessorClient();
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_VALUE = "application/json";

    private final HttpClient httpclient;
    private final URI defaultURI;
    private final URI fallbackURI;
    private final long backoff;
    private final int retry;

    private PaymentProcessorClient() {
        this.httpclient = HttpClientConfig.getInstance().getHttpClient();

        var defaultURL = System.getenv().getOrDefault("HOST_PROCESSOR_DEFAULT", "http://localhost:8001");
        var fallbackURL = System.getenv().getOrDefault("HOST_PROCESSOR_FALLBACK", "http://localhost:8002");
        this.defaultURI = URI.create(defaultURL.concat("/payments"));
        this.fallbackURI = URI.create(fallbackURL.concat("/payments"));

        this.backoff = Integer.parseInt(System.getenv().getOrDefault("NUM_BACK_OFF", "15")) * 1_000_000L;
        this.retry = Integer.parseInt(System.getenv().getOrDefault("NUM_RETRY", "15"));

        System.out.println("======================================");
        System.out.println("============ Loading URLs ============");
        System.out.println("======================================");
        System.out.println("default: " + defaultURL);
        System.out.println("fallback: " + fallbackURL);
        System.out.println("retry: " + this.backoff);
        System.out.println("backoff: " + this.retry);
    }

    public static PaymentProcessorClient getInstance() {
        return INSTANCE;
    }

    public boolean processPayment(PaymentRequest paymentRequest) {
        var request = buildRequest(paymentRequest.getPostData(), defaultURI);

        return sendPayment(request);
//
        //for (int i = 0; i < retry; i++) {
        //    if (sendPayment(request)) {
        //        return true;
        //    }
//
        //    LockSupport.parkNanos(this.backoff);
        //}
//
        //return false;
    }

    public boolean sendPaymentFallback(PaymentRequest paymentRequest) {
        var request = buildRequest(paymentRequest.getPostData(), fallbackURI);
        return sendPayment(request);
    }

    private boolean sendPayment(HttpRequest request) {
        try {
            var response = this.httpclient.send(request, HttpResponse.BodyHandlers.discarding());
            int sc = response.statusCode();
            return sc == 200;
        } catch (Exception _) {
            return false;
        }
    }

    private HttpRequest buildRequest(byte[] body, URI uri) {
        return HttpRequest.newBuilder()
                .uri(uri)
                .header(CONTENT_TYPE, CONTENT_TYPE_VALUE)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
    }
}
