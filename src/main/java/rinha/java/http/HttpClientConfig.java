package rinha.java.http;

import java.net.http.HttpClient;

public final class HttpClientConfig {
    private static final HttpClientConfig INSTANCE = new HttpClientConfig();

    private final HttpClient httpClient;

    private HttpClientConfig() {
        this.httpClient = HttpClient.newBuilder()
                .followRedirects(java.net.http.HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_2)
                .executor(Runnable::run)
                .build();
    }

    public static HttpClientConfig getInstance() {
        return INSTANCE;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }
}
