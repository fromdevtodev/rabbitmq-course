package com.fromdevtodev.rabbitmq.broker;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                       declare the queue notifications.email
 * checkout --mode sync|async [options]        the order service: one checkout per order
 * work     [options]                          the email worker (the consumer)
 * reset                                       delete the queue
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
            case "setup" -> Setup.run();
            case "checkout" -> checkout(options);
            case "work" -> EmailWorker.run(
                    options.text("name", "email-worker"),
                    provider(options),
                    options.number("limit", 0));
            case "reset" -> Topology.reset();
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    // region readme:command
    private static void checkout(Args options) throws Exception {
        Mode mode = Mode.parse(options.required("mode"));
        EmailProvider provider = provider(options);
        long orders = options.number("orders", 1);
        long first = options.number("first", 1);
        long intervalMillis = options.number("interval-ms", 0);
        Log.info("order-service", mode + " checkout, email provider " + provider.describe());
        if (mode == Mode.ASYNC) {
            try (RabbitPublisher publisher = new RabbitPublisher()) {
                run(new CheckoutService(mode, provider, publisher), orders, first, intervalMillis);
            }
        } else {
            run(new CheckoutService(mode, provider, event -> { throw new IllegalStateException("sync mode never publishes"); }),
                    orders, first, intervalMillis);
        }
    }
    // endregion

    /** One checkout per order; with an interval, order {@code i} starts {@code i * interval} after the first. */
    private static void run(CheckoutService service, long orders, long first, long intervalMillis) throws InterruptedException {
        long begin = System.nanoTime();
        for (long i = 0; i < orders; i++) {
            long wait = i * intervalMillis - (System.nanoTime() - begin) / 1_000_000;
            if (wait > 0) {
                Thread.sleep(wait);
            }
            CheckoutService.Answer answer = service.checkout(OrderEvent.of(first + i));
            if (answer.ok()) {
                Log.info("customer", answer.orderId() + ": answered in " + answer.millis() + " ms");
            } else {
                Log.info("customer", answer.orderId() + ": ERROR after " + answer.millis() + " ms: " + answer.detail());
            }
        }
    }

    private static EmailProvider provider(Args options) {
        String state = options.text("provider", "up");
        if (!state.equals("up") && !state.equals("down")) {
            throw new IllegalArgumentException("--provider must be up or down");
        }
        return new EmailProvider(options.number("provider-ms", 2_000), state.equals("down"));
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                        declare the queue notifications.email
                  checkout --mode sync|async                   the order service
                           [--orders 1] [--first 1]            how many orders, and the number of the first
                           [--interval-ms 0]                   start one order every N ms (0 = back to back)
                           [--provider up|down]                state of the email provider
                           [--provider-ms 2000]                how long the provider takes per email
                  work     [--provider-ms 2000]                the email worker; it sends one email per message
                           [--name email-worker]               connection name, shown in the management UI
                           [--limit 0]                         stop after N messages (0 = until Ctrl+C)
                  reset                                        delete the queue

                With Maven:  mvn -q compile exec:java -Dexec.args="checkout --mode async --orders 3"
                """);
    }
}
