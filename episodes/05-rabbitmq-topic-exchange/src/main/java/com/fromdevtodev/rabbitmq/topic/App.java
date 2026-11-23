package com.fromdevtodev.rabbitmq.topic;

import java.util.List;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                            declare events and the four queues of experiment 1
 * setup-comparison                                 declare events and the three queues of experiment 2
 * publish [--key orders.eu.created] [--first 1]    publish one event to the exchange events and go away
 * explain [--key orders.eu.created] [--set routing]  which queues the rule predicts (no broker needed)
 * reset                                            delete the exchange and the seven queues
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
            case "setup" -> Setup.routing();
            case "setup-comparison" -> Setup.comparison();
            case "publish" -> Publisher.publish(
                    options.text("key", "orders.eu.created"),
                    options.number("count", 1),
                    options.number("first", 1));
            case "explain" -> explain(options.text("key", "orders.eu.created"), options.text("set", "routing"));
            case "reset" -> Topology.reset();
            // endregion
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static void explain(String routingKey, String set) {
        List<Binding> bindings = set.equals("comparison") ? Topology.COMPARISON : Topology.ROUTING;
        var queues = TopicRule.queuesFor(routingKey, bindings);
        System.out.println("routing key " + routingKey + " -> " + (queues.isEmpty() ? "no queue" : String.join(", ", queues)));
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                      declare the exchange events, four queues and four patterns (experiment 1)
                  setup-comparison                           declare the exchange events, three queues and three patterns (experiment 2)
                  publish   [--key orders.eu.created]        the routing key: words separated by dots, no wildcards
                            [--count 1] [--first 1]          how many events, and the number of the first (evt_0001, ...)
                  explain   [--key orders.eu.created]        the queues that the rule predicts for this key (no broker needed)
                            [--set routing]                  routing (experiment 1) or comparison (experiment 2)
                  reset                                      delete the exchange and the seven queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key payments.eu.failed --first 3"
                """);
    }
}
