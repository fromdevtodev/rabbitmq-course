package com.fromdevtodev.rabbitmq.dlx;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Entry point.
 *
 * <pre>
 * setup [--stage all|dead-letter|source]        declare the topology (dead-letter side first, then the work queue)
 *       [--dlq-key order.created]               the key the dead-letter queue is bound with (the demo that keeps nothing uses order.failed)
 *       [--dead-letter on|off] [--max-length 0] [--message-ttl-ms 0]   settings of the work queue for the experiments
 * publish [--first 1] [--count 3] [--bad 2]     publish orders; the event numbers in --bad have no email address
 * work [--name email-worker] [--work-ms 700] [--max-deliveries 0] [--release-file f]   the email worker: nack with requeue false on a bad order
 *                                               (--release-file is a recording aid: delivery N waits until the file has N lines)
 * inspect                                       print why the first message of the dead-letter queue died (and put it back)
 * reset                                         delete both queues and both exchanges
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
            case "setup" -> Setup.run(
                    options.text("stage", "all"),
                    options.text("dlq-key", Topology.ROUTING_KEY),
                    new Topology.Options(
                            options.text("dead-letter", "on").equals("on"),
                            options.number("max-length", 0),
                            options.number("message-ttl-ms", 0)));
            case "publish" -> Publisher.publish(
                    options.number("first", 1),
                    options.number("count", 3),
                    numbers(options.text("bad", "2")));
            case "work" -> EmailWorker.run(
                    options.text("name", "email-worker"),
                    options.number("work-ms", 700),
                    options.number("max-deliveries", 0),
                    options.text("release-file", ""));
            case "inspect" -> Inspector.inspect();
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

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup     [--stage all|dead-letter|source]   dead-letter side (orders.dlx + notifications.email.dlq) and/or the work queue
                            [--dlq-key order.created]          binding key of the dead-letter queue
                            [--dead-letter on|off]             whether the work queue names orders.dlx (x-dead-letter-exchange)
                            [--max-length 0] [--message-ttl-ms 0]   optional queue limits (0: none), for the experiments
                  publish   [--first 1] [--count 3] [--bad 2]  orders to publish; the numbers in --bad have no email address
                  work      [--name email-worker] [--work-ms 700] [--max-deliveries 0]
                            [--release-file f]               recording aid: delivery N waits until f has N lines
                  inspect                                      why did the first dead letter die? (x-death), and put it back
                  reset                                        delete both queues and both exchanges

                With Maven:  mvn -q compile exec:java -Dexec.args="setup --stage all"
                """);
    }
}
