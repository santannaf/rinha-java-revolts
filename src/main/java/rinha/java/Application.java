package rinha.java;

import com.sun.net.httpserver.HttpServer;
import rinha.java.http.HttpClientConfig;
import rinha.java.http.PathHandler;
import rinha.java.persistence.redis.RedisPool;
import rinha.java.service.PaymentProcessorClient;
import rinha.java.worker.ProcessWorker;

import java.io.IOException;
import java.net.InetSocketAddress;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class Application {
    public static void main(String[] args) throws IOException, InterruptedException {
        int serverPort = 8080;
        String serverHost = "0.0.0.0";
        int backlog = 8192;

        HttpServer server = HttpServer.create(new InetSocketAddress(serverHost, serverPort), backlog);
        server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());

        var handlers = PathHandler.getInstance();
        var ignored = HttpClientConfig.getInstance();
        server.createContext("/", handlers.getHandlers());

        RedisPool.getInstance();
        ProcessWorker.getInstance();
        PaymentProcessorClient.getInstance();

        server.start();

        System.out.println("Java Server Revolts on http://" + serverHost + ":" + serverPort);
        Thread.currentThread().join();
    }
}
