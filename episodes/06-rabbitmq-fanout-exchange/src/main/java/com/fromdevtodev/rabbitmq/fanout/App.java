package com.fromdevtodev.rabbitmq.fanout;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                            declare price-updates and the three queues of experiment 1
 * setup-shared                                     declare price-updates and the one queue of experiment 2, design one
 * setup-empty                                      declare price-updates and nothing else (experiment 3)
 * publish [--key toys] [--count 1] [--first 1]     publish price changes to the exchange price-updates and go away
 * consume --name cache-a [--queue cache.shared]    a consumer of a queue that others may share (runs until Ctrl+C)
 * instance --name instance-1                       an instance with its own exclusive queue (runs until Ctrl+C)
 * explain [--key toys]                             which queues the rule predicts for experiment 1 (no broker needed)
 * reset                                            delete the exchange and the four named queues
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
            case "setup" -> Setup.broadcast();
            case "setup-shared" -> Setup.shared();
            case "setup-empty" -> Setup.empty();
            case "publish" -> Publisher.publish(
                    options.text("key", "toys"),
                    options.number("count", 1),
                    options.number("first", 1));
            case "consume" -> Consumers.onSharedQueue(options.required("name"), options.text("queue", Topology.SHARED));
            case "instance" -> Consumers.instance(options.required("name"));
            case "explain" -> explain(options.text("key", "toys"));
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
        var queues = FanoutRule.queuesFor(routingKey, Topology.BROADCAST);
        System.out.println("routing key " + routingKey + " -> " + (queues.isEmpty() ? "no queue" : String.join(", ", queues)));
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                      declare the exchange price-updates (fanout) and three queues (experiment 1)
                  setup-shared                               declare the exchange and one queue, cache.shared (experiment 2, design one)
                  setup-empty                                declare the exchange and no queue (experiment 3)
                  publish   [--key toys]                     the routing key: a fanout exchange ignores it
                            [--count 1] [--first 1]          how many price changes, and the number of the first (evt_0001, ...)
                  consume   --name cache-a                   a consumer of a queue that others may share (runs until Ctrl+C)
                            [--queue cache.shared]
                  instance  --name instance-1                an instance with its own exclusive queue, named by the broker (until Ctrl+C)
                  explain   [--key toys]                     the queues that the rule predicts (no broker needed)
                  reset                                      delete the exchange and the four named queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key toys --first 1"
                """);
    }
}
