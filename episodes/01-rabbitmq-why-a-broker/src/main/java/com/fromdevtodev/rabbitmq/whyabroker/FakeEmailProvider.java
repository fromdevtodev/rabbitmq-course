package com.fromdevtodev.rabbitmq.whyabroker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fake external email provider: slow, and you can switch it off.
 *
 * <pre>
 * POST /send?orderId=ord_1042     waits {@code latency}, counts the email as accepted, answers 202 (503 when down)
 * POST /admin/down                from now on every call answers 503 immediately
 * POST /admin/up                  back to normal
 * POST /admin/latency/{millis}    change how long /send takes
 * GET  /admin/stats               how many emails were accepted per order (a count above 1 is a duplicate)
 * </pre>
 *
 * <p>No email is sent anywhere. The provider counts an email as accepted <em>after</em> its delay, even if the
 * caller has already given up waiting. That is what makes a client timeout an unknown outcome.
 */
public final class FakeEmailProvider implements AutoCloseable {
    private final HttpServer server;
    private final Map<String, AtomicInteger> accepted = new ConcurrentHashMap<>();
    private volatile boolean down;
    private volatile long latencyMillis;

    private FakeEmailProvider(HttpServer server, long latencyMillis) {
        this.server = server;
        this.latencyMillis = latencyMillis;
    }

    public static FakeEmailProvider start(int port, long latencyMillis) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        // Virtual threads: thousands of sleeping requests cost nothing, so the provider never limits the demo.
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        FakeEmailProvider provider = new FakeEmailProvider(server, latencyMillis);
        server.createContext("/send", provider::send);
        server.createContext("/admin/", provider::admin);
        server.start();
        return provider;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public int acceptedFor(String orderId) {
        AtomicInteger count = accepted.get(orderId);
        return count == null ? 0 : count.get();
    }

    public int acceptedTotal() {
        return accepted.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    public void setDown(boolean down) {
        this.down = down;
    }

    private void send(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"POST".equals(exchange.getRequestMethod())) {
                reply(exchange, 405, "POST only");
                return;
            }
            String orderId = queryParam(exchange, "orderId");
            if (down) {
                Log.info("provider", "DOWN: refused email for " + orderId);
                reply(exchange, 503, "provider is down");
                return;
            }
            Log.info("provider", "received email request for " + orderId + " (takes " + latencyMillis + " ms)");
            try {
                Thread.sleep(latencyMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            int count = accepted.computeIfAbsent(orderId, k -> new AtomicInteger()).incrementAndGet();
            Log.info("provider", "accepted email for " + orderId + (count > 1 ? "  <-- DUPLICATE (#" + count + ")" : ""));
            try {
                reply(exchange, 202, "accepted");
            } catch (IOException callerGaveUp) {
                Log.info("provider", "the caller had already stopped waiting; the email for " + orderId + " was still accepted");
            }
        }
    }

    private void admin(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            if ("/admin/down".equals(path)) {
                down = true;
                Log.info("provider", "switched DOWN");
                reply(exchange, 200, "provider is down");
            } else if ("/admin/up".equals(path)) {
                down = false;
                Log.info("provider", "switched UP");
                reply(exchange, 200, "provider is up");
            } else if (path.startsWith("/admin/latency/")) {
                latencyMillis = Long.parseLong(path.substring("/admin/latency/".length()));
                Log.info("provider", "latency is now " + latencyMillis + " ms");
                reply(exchange, 200, "latency is " + latencyMillis + " ms");
            } else if ("/admin/stats".equals(path)) {
                StringBuilder sb = new StringBuilder();
                new TreeMap<>(accepted).forEach((id, n) -> sb.append(id).append(": ").append(n).append('\n'));
                sb.append("total accepted: ").append(acceptedTotal()).append(down ? "  (provider is DOWN)" : "").append('\n');
                reply(exchange, 200, sb.toString());
            } else {
                reply(exchange, 404, "unknown admin path");
            }
        } catch (NumberFormatException e) {
            Log.info("provider", "bad latency value");
        }
    }

    private static String queryParam(HttpExchange exchange, String name) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query != null) {
            for (String pair : query.split("&")) {
                String[] kv = pair.split("=", 2);
                if (kv.length == 2 && kv[0].equals(name)) {
                    return java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
                }
            }
        }
        return "unknown";
    }

    private static void reply(HttpExchange exchange, int status, String text) throws IOException {
        byte[] body = text.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
