package com.fromdevtodev.rabbitmq.retry;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Entry point.
 *
 * <pre>
 * setup                                    the work queue
 * build-retry [--delay-ms 10000]            the retry exchange, the retry queue (message TTL) and its binding
 * publish [--first 1] [--count 3]            publish orders
 * work [--on-failure requeue|sleep|retry-queue] [--work-ms 300] [--down-events 2] [--down-seconds 25]
 *      [--sleep-ms 10000] [--max-deliveries 0] [--name email-worker]
 * explain [--on-failure requeue]             the answer the worker gives (no broker needed)
 * reset                                      delete both queues and both exchanges
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
        // region readme:commands
        switch (args[0]) {
            case "setup" -> Setup.run();
            case "build-retry" -> Setup.buildRetry(options.number("delay-ms", Topology.retryDelayMillis()));
            case "publish" -> Publisher.publish(options.number("first", 1), options.number("count", 3));
            case "work" -> EmailWorker.run(
                    options.text("name", "email-worker"),
                    parse(options.text("on-failure", "requeue")),
                    options.number("work-ms", 300),
                    options.number("sleep-ms", 10000),
                    events(options.text("down-events", "2")),
                    options.number("down-seconds", 25) * 1000,
                    options.number("max-deliveries", 0));
            case "explain" -> explain(options.text("on-failure", "requeue"));
            case "reset" -> Topology.reset();
            // endregion
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static Rules.OnFailure parse(String text) {
        return Rules.OnFailure.valueOf(text.toUpperCase().replace('-', '_'));
    }

    /** "2,5" means evt_0002 and evt_0005. */
    private static Set<String> events(String commaSeparated) {
        return Arrays.stream(commaSeparated.split(",")).map(String::trim).filter(text -> !text.isEmpty())
                .map(number -> OrderEvent.of(Long.parseLong(number)).eventId()).collect(Collectors.toSet());
    }

    private static void explain(String onFailure) {
        Rules.Outcome outcome = Rules.decide(Rules.Failure.MAIL_SERVER_DOWN, parse(onFailure));
        System.out.println(Rules.Failure.MAIL_SERVER_DOWN.reason() + " -> " + outcome + " (requeue=" + Rules.requeue(outcome) + ")");
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                       declare the exchange orders, the queue notifications.email and the binding
                  build-retry [--delay-ms 10000]              add orders.retry, the retry queue (message TTL) and its binding
                  publish   [--first 1] [--count 3]           how many orders, and the number of the first (evt_0001 / ord_1041, ...)
                  work      [--name email-worker]             the email worker: manual ack, one message at a time
                            [--on-failure requeue|sleep|retry-queue]   what it does when the mail server is down
                            [--work-ms 300]                   how long "sending the email" takes
                            [--down-events 2]                 the events whose recipient's mail server is down (comma separated)
                            [--down-seconds 25]                for how long (seconds), counted from the first call that reaches it
                            [--sleep-ms 10000]           the sleep of --on-failure sleep (ms)
                            [--max-deliveries 0]              stop after this many deliveries (0: until Ctrl+C)
                  explain   [--on-failure requeue|sleep|retry-queue]
                  reset                                       delete both queues and both exchanges

                With Maven:  mvn -q compile exec:java -Dexec.args="work --on-failure retry-queue"
                """);
    }
}
