package rinha.java.warmup;

import rinha.java.http.HttpClientConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public final class WarmUp {
    private static final WarmUp INSTANCE = new WarmUp();

    private final URI defaultURI;
    private final HttpClient httpClient;

    private WarmUp() {
        this.defaultURI = URI.create("http://0.0.0.0:8080/payments");
        this.httpClient = HttpClientConfig.getInstance().getHttpClient();
        start();
    }

    @SuppressWarnings("UnusedReturnValue")
    public static WarmUp getInstance() {
        return INSTANCE;
    }

    private void start() {
        String jsonBody = "{\"correlationId\": \"4a7901b8-7d26-4d9d-aa19-4dc1c7cf60b3\", \"amount\": 0 }";
        var request = HttpRequest.newBuilder(defaultURI).POST(HttpRequest.BodyPublishers.ofString(jsonBody)).build();

        for (int i = 0; i < 1; i++) {
            try {
                httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            } catch (Exception ignored) {
            }
        }
    }
}
