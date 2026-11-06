package com.fromdevtodev.rabbitmq.whyabroker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@code POST /orders} for the order service.
 *
 * <p>Requests run on a <strong>fixed pool of platform threads</strong> on purpose: a request that waits keeps its
 * thread, just like in a typical thread-per-request server. With a small pool you can watch the "capacity"
 * problem from the video (see the README).
 */
public final class OrderServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService pool;
    private final OrderService orders;

    private OrderServer(HttpServer server, ExecutorService pool, OrderService orders) {
        this.server = server;
        this.pool = pool;
        this.orders = orders;
    }

    public static OrderServer start(int port, int threads, OrderService orders) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        server.setExecutor(pool);
        OrderServer self = new OrderServer(server, pool, orders);
        server.createContext("/orders", self::handle);
        server.start();
        return self;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"POST".equals(exchange.getRequestMethod())) {
                reply(exchange, 405, "{\"error\":\"POST only\"}");
                return;
            }
            String customer = "cus_88";
            String query = exchange.getRequestURI().getRawQuery();
            if (query != null && query.startsWith("customer=") && query.length() > "customer=".length()) {
                customer = query.substring("customer=".length()).replaceAll("[^A-Za-z0-9_-]", "");
            }
            long started = System.nanoTime();
            try {
                Order order = orders.place(customer);
                Log.info("order-service", order.id() + " created, answering 201 after " + millisSince(started) + " ms");
                reply(exchange, 201, "{\"orderId\":\"" + order.id() + "\",\"status\":\"created\"}");
            } catch (FollowUpFailedException e) {
                Log.info("order-service", e.order().id() + " was saved, but the follow-up failed after "
                        + millisSince(started) + " ms: " + e.getCause().getMessage() + " -> answering 502");
                reply(exchange, 502, "{\"orderId\":\"" + e.order().id() + "\",\"orderSaved\":true,"
                        + "\"error\":\"the follow-up step failed; the order exists, the outcome of the email is unknown\"}");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static long millisSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    private static void reply(HttpExchange exchange, int status, String json) throws IOException {
        byte[] body = (json + "\n").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    @Override
    public void close() {
        server.stop(0);
        pool.shutdownNow();
    }
}
