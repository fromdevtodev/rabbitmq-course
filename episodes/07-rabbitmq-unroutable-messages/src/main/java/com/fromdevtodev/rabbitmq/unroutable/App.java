package com.fromdevtodev.rabbitmq.unroutable;

import java.util.Set;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                                declare orders, three queues and five bindings (experiments 1 and 2)
 * setup-parking                                        the same, plus orders.unrouted and unrouted.parking (experiment 3)
 * publish [--key order.canceled] [--first 1] [--count 1] [--mandatory true]   publish order events to orders and go away
 * explain [--key order.canceled]                       which queues the rule predicts (no broker needed)
 * reset                                                delete both exchanges and the four queues
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
            case "setup" -> Setup.orders();
            case "setup-parking" -> Setup.ordersWithParking();
            case "publish" -> Publisher.publish(
                    options.text("key", "order.canceled"),
                    options.number("count", 1),
                    options.number("first", 1),
                    Boolean.parseBoolean(options.text("mandatory", "false")));
            case "explain" -> explain(options.text("key", "order.canceled"));
            case "reset" -> Topology.reset();
            // endregion
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static void explain(String routingKey) {
        Set<String> queues = DirectRule.queuesFor(routingKey, Topology.ORDERS);
        System.out.println("routing key " + routingKey + " -> " + (queues.isEmpty() ? "no queue: unroutable" : String.join(", ", queues)));
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                      declare the exchange orders, three queues and five bindings
                  setup-parking                              the same, plus the exchange orders.unrouted and the queue unrouted.parking
                  publish   [--key order.canceled]           the routing key; the bindings use order.cancelled (two Ls)
                            [--count 1] [--first 1]          how many events, and the number of the first (evt_0001, ...)
                            [--mandatory false]              true: ask the broker to return a message that matches no binding
                  explain   [--key order.canceled]           the queues that the rule predicts for this key (no broker needed)
                  reset                                      delete both exchanges and the four queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key order.canceled --mandatory true"
                """);
    }
}
