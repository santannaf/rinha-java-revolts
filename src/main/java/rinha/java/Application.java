package rinha.java;

import com.sun.net.httpserver.HttpServer;
import rinha.java.http.HttpClientConfig;
import rinha.java.http.PathHandler;
import rinha.java.leadership.LeaderHealthMonitor;
import rinha.java.persistence.redis.read.RedisPrincipalReadClient;
import rinha.java.persistence.redis.read.RedisSecondaryReadClient;
import rinha.java.persistence.redis.write.RedisPrincipalWriteClient;
import rinha.java.persistence.redis.write.RedisSecondaryWriteClient;
import rinha.java.service.PaymentProcessorClient;
import rinha.java.warmup.WarmUp;
import rinha.java.worker.ProcessWorker;

import java.io.IOException;
import java.net.InetSocketAddress;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class Application {
    public static void main(String[] args) throws IOException, InterruptedException {
        int serverPort = 8080;
        String serverHost = "0.0.0.0";
        int backlog = 4096;

        HttpServer server = HttpServer.create(new InetSocketAddress(serverHost, serverPort), backlog);

        var handlers = PathHandler.getInstance();
        var ignored = HttpClientConfig.getInstance();
        server.createContext("/", handlers.getHandlers());

        RedisPrincipalWriteClient.getInstance();
        RedisSecondaryWriteClient.getInstance();
        RedisPrincipalReadClient.getInstance();
        RedisSecondaryReadClient.getInstance();
        LeaderHealthMonitor.getInstance();
        ProcessWorker.getInstance();
        PaymentProcessorClient.getInstance();

        server.start();

        //WarmUp.getInstance();
        Thread.startVirtualThread(() -> LeaderHealthMonitor.getInstance().start());

        System.out.println("Java Server Revolts on http://" + serverHost + ":" + serverPort);

        Thread.currentThread().join();
    }
}
