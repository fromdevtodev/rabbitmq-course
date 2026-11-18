package com.fromdevtodev.rabbitmq.direct;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                                declare logs, errors-pager, audit, metrics and the five bindings
 * declare [--queue notifications.email]                declare one queue and no binding (the default exchange's demo)
 * publish [--key error] [--count 1] [--first 1]        publish log lines to the exchange logs and go away
 * publish-default [--queue notifications.email]        publish to the default exchange (the empty name)
 * bind-default [--queue notifications.email]           try to add a binding to the default exchange
 * explain [--key warning]                              which queues the rule of a direct exchange predicts (no broker needed)
 * reset                                                delete the exchange and the four queues
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
            case "declare" -> Setup.queueOnly(options.text("queue", Topology.EMAIL));
            case "publish" -> Publisher.publish(
                    options.text("key", LogEvent.ERROR),
                    options.number("count", 1),
                    options.number("first", 1));
            case "publish-default" -> Publisher.publishToDefault(
                    options.text("queue", Topology.EMAIL),
                    options.number("first", 1));
            case "bind-default" -> DefaultExchange.tryToBind(options.text("queue", Topology.EMAIL), options.text("key", "audit"));
            case "explain" -> explain(options.text("key", LogEvent.WARNING));
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
        var queues = DirectRule.queuesFor(routingKey, Topology.BINDINGS);
        System.out.println("routing key " + routingKey + " -> " + (queues.isEmpty() ? "no queue" : String.join(", ", queues)));
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                        declare the exchange logs, errors-pager, audit, metrics and their bindings
                  declare         [--queue notifications.email]  declare one queue, with no binding of ours
                  publish         [--key error]                the routing key (error, warning, info, debug...)
                                  [--count 1] [--first 1]      how many lines, and the number of the first (log_0001, ...)
                  publish-default [--queue notifications.email] [--first 1]
                                                               publish to the default exchange, with this routing key
                  bind-default    [--queue notifications.email] [--key audit]
                                                               try to bind the queue to the default exchange (the broker refuses)
                  explain         [--key warning]              the queues that the rule predicts for this key (no broker needed)
                  reset                                        delete the exchange and the four queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key warning --first 2"
                """);
    }
}
