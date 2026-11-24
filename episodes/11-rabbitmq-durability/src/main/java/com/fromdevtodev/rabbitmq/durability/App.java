package com.fromdevtodev.rabbitmq.durability;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                                        declare the durable queues notifications.email and loyalty.points
 * publish [--queue notifications.email] [--count 1]            publish order events and go away
 *         [--first 1] [--persistent false]                     (persistent true: delivery mode 2; false: no properties at all)
 * explain [--durable true] [--persistent false]                what a restart leaves, by the rule (no broker needed)
 * reset                                                        delete both queues
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
                    options.text("queue", Topology.EMAIL),
                    options.number("count", 1),
                    options.number("first", 1),
                    options.flag("persistent", false));
            case "explain" -> explain(options.flag("durable", true), options.flag("persistent", false));
            case "reset" -> Topology.reset();
            // endregion
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static void explain(boolean durableQueue, boolean persistentMessage) {
        System.out.println("durable queue: " + durableQueue + ", persistent message: " + persistentMessage
                + " -> after a restart, " + RestartRule.afterRestart(durableQueue, persistentMessage).description);
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                         declare the durable queues notifications.email and loyalty.points
                  publish  [--queue notifications.email]        the queue to publish to (notifications.email or loyalty.points)
                           [--count 1] [--first 1]              how many events, and the number of the first (evt_0001, ...)
                           [--persistent false]                 true: persistent messages; false: no properties, transient messages
                  explain  [--durable true] [--persistent false]  what a restart leaves, by the rule (no broker needed)
                  reset                                         delete both queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --count 3 --persistent true"
                """);
    }
}
