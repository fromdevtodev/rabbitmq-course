package com.fromdevtodev.rabbitmq.whyabroker;

import java.net.URI;
import java.time.Duration;

/**
 * Entry point. Three processes, started in three terminals:
 *
 * <pre>
 * provider        the fake, slow email provider (HTTP, port 9090)
 * order-service   POST /orders, in --mode inline or --mode broker (HTTP, port 8080)
 * worker          the consumer that sends the emails (needs RabbitMQ)
 * </pre>
 */
public final class App {
    private App() { }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args[0].equals("--help") || args[0].equals("help")) {
            usage();
            return;
        }
        Args options = new Args(args, 1);
        switch (args[0]) {
            case "provider" -> provider(options);
            case "order-service" -> orderService(options);
            case "worker" -> worker(options);
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static void provider(Args options) throws Exception {
        int port = (int) options.number("port", 9090);
        long latency = options.number("latency-ms", 12_000);
        FakeEmailProvider provider = FakeEmailProvider.start(port, latency);
        Log.info("provider", "listening on http://localhost:" + provider.port() + " (each email takes " + latency + " ms)");
        Thread.currentThread().join();
    }

    private static void orderService(Args options) throws Exception {
        String mode = options.string("mode", "inline");
        int port = (int) options.number("port", 8080);
        int threads = (int) options.number("threads", 16);
        long commitMillis = options.number("db-ms", 40);
        URI provider = URI.create(options.string("provider", "http://localhost:9090"));
        Duration emailTimeout = Duration.ofMillis(options.number("email-timeout-ms", 8_000));

        OrderFollowUp followUp;
        switch (mode) {
            case "inline" -> {
                EmailClient email = new HttpEmailClient(provider, emailTimeout);
                followUp = email::sendConfirmation;           // the customer waits for the provider
            }
            case "broker" -> {
                RabbitOrderEventPublisher publisher = new RabbitOrderEventPublisher();
                followUp = publisher::orderCreated;            // the customer waits for a publish only
            }
            default -> throw new IllegalArgumentException("--mode must be 'inline' or 'broker', got: " + mode);
        }
        OrderService orders = new OrderService(new OrderRepository(Duration.ofMillis(commitMillis)), followUp);
        OrderServer server = OrderServer.start(port, threads, orders);
        Log.info("order-service", "mode=" + mode + ", " + threads + " request threads, listening on http://localhost:"
                + server.port() + "/orders");
        Thread.currentThread().join();
    }

    private static void worker(Args options) throws Exception {
        URI provider = URI.create(options.string("provider", "http://localhost:9090"));
        Duration emailTimeout = Duration.ofMillis(options.number("email-timeout-ms", 30_000));
        Duration retryDelay = Duration.ofMillis(options.number("retry-ms", 2_000));
        RabbitEmailWorker.run(new HttpEmailClient(provider, emailTimeout), retryDelay);
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  provider        [--port 9090] [--latency-ms 12000]
                  order-service   [--mode inline|broker] [--port 8080] [--threads 16] [--db-ms 40]
                                  [--provider http://localhost:9090] [--email-timeout-ms 8000]
                  worker          [--provider http://localhost:9090] [--retry-ms 2000] [--email-timeout-ms 30000]

                With Maven:  mvn -q compile exec:java -Dexec.args="provider"
                """);
    }
}
