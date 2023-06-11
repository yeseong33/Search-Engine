package edu.hanyang.httpserver;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SimpleHttpServer {
    public static int port = 9000;

    private HttpServer server;

    public void start(int port, HttpHandler handler) throws Exception {
        try {
            // Executor executor = Executors.newFixedThreadPool(4);

            SimpleHttpServer.port = port;
            server = HttpServer.create(new InetSocketAddress("0.0.0.0", SimpleHttpServer.port), 0);
            System.out.println("server started at port " + SimpleHttpServer.port);
            server.createContext("/", handler);
            server.setExecutor(null);
            server.start();
        } catch (IOException exc) {
            exc.printStackTrace();
        }
    }

    public void stop() {
        server.stop(0);
        System.out.println("server stopped");
    }
}
