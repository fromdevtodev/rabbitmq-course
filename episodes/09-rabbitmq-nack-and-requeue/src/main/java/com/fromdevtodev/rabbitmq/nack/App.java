package com.fromdevtodev.rabbitmq.nack;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Entry point. Run the worker in its own terminal:
 *
 * <pre>
 * setup                                                declare the exchange orders, the queue notifications.email and the binding
 * publish [--first 1] [--count 3] [--bad 2]            publish orders; the event numbers in --bad have no email address
 * work [--name email-worker] [--on-invalid requeue]    the email worker: one message at a time, manual ack, nack on failure
 *      [--work-ms 700] [--max-deliveries 0]
 * explain [--failure invalid-order] [--on-invalid requeue]   the answer the worker gives (no broker needed)
 * reset                                                delete the queue and the exchange
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
            case "publish" -> Publisher.publish(
                    options.number("first", 1),
                    options.number("count", 3),
                    numbers(options.text("bad", "2")));
            case "work" -> EmailWorker.run(
                    options.text("name", "email-worker"),
                    Rules.OnInvalid.valueOf(options.text("on-invalid", "requeue").toUpperCase()),
                    options.number("work-ms", 700),
                    options.number("max-deliveries", 0));
            case "explain" -> explain(options.text("failure", "invalid-order"), options.text("on-invalid", "requeue"));
            case "reset" -> Topology.reset();
            // endregion
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static Set<Long> numbers(String commaSeparated) {
        return Arrays.stream(commaSeparated.split(","))
                .map(String::trim).filter(text -> !text.isEmpty()).map(Long::parseLong).collect(Collectors.toSet());
    }

    private static void explain(String failure, String onInvalid) {
        Rules.Failure kind = Rules.Failure.valueOf(failure.toUpperCase().replace('-', '_'));
        Rules.Outcome outcome = Rules.decide(kind, Rules.OnInvalid.valueOf(onInvalid.toUpperCase()));
        System.out.println(kind.reason() + " -> " + outcome + (outcome == Rules.Outcome.ACK ? "" : " (requeue=" + Rules.requeue(outcome) + ")"));
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                      declare the exchange orders, the queue notifications.email and the binding
                  publish   [--first 1] [--count 3]          how many orders, and the number of the first (evt_0001 / ord_1041, ...)
                            [--bad 2]                        the event numbers published WITHOUT an email address (comma separated; "" for none)
                  work      [--name email-worker]            the email worker: manual ack, one message at a time
                            [--on-invalid requeue|discard]   what it answers to an order with no email address: nack with requeue true or false
                            [--work-ms 700]                  how long "sending the email" takes
                            [--max-deliveries 0]             stop after this many deliveries (0: until Ctrl+C)
                  explain   [--failure invalid-order|mail-server-down|none] [--on-invalid requeue|discard]
                  reset                                      delete the queue and the exchange

                With Maven:  mvn -q compile exec:java -Dexec.args="work --on-invalid discard"
                """);
    }
}
