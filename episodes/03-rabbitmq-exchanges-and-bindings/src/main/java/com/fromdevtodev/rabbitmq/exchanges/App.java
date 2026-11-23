package com.fromdevtodev.rabbitmq.exchanges;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                                 declare orders, notifications.email and audit.log, and their bindings
 * publish [--key order.created] [--count 1] [--first 1] the producer: publish order events to the exchange and go away
 * bind-points                                           the points service: declare loyalty.points and bind it to orders
 * reset                                                 delete the exchange and the three queues
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
                    options.text("key", OrderEvent.CREATED),
                    options.number("count", 1),
                    options.number("first", 1));
            case "bind-points" -> PointsWorker.bind();
            case "reset" -> Topology.reset();
            // endregion
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                        declare the exchange orders, notifications.email, audit.log and their bindings
                  publish  [--key order.created]               the routing key of the message (order.created or order.cancelled)
                           [--count 1] [--first 1]             how many events, and the number of the first (evt_0001, ...)
                  bind-points                                  declare loyalty.points and bind it to orders with order.created
                  reset                                        delete the exchange and the three queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key order.created --first 2"
                """);
    }
}
