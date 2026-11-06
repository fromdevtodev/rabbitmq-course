package com.fromdevtodev.rabbitmq.exchanges;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                          declare the exchange, notifications.email, audit.log and their bindings
 * publish  [--key order.created] [--count 1] [--first 1]    publish to the exchange "orders" with a routing key
 * bind     --queue loyalty.points --key order.created        declare a queue and bind it to the exchange
 * unbind   --queue loyalty.points --key order.created        remove one binding
 * consume  --queue loyalty.points [--limit 0]                take messages from one queue and print them
 * status   [--samples 1] [--interval-ms 1000]                print "ready" for each of the three queues
 * reset                                                      delete the exchange and the three queues
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
            case "setup" -> Bindings.setup();
            case "publish" -> Publisher.run(
                    options.text("key", Topology.ORDER_CREATED), options.number("count", 1), options.number("first", 1));
            case "bind" -> Bindings.bind(options.required("queue"), options.required("key"));
            case "unbind" -> Bindings.unbind(options.required("queue"), options.required("key"));
            case "consume" -> Consumer.run(options.required("queue"), options.number("limit", 0));
            case "status" -> QueueStatus.run(options.number("samples", 1), options.number("interval-ms", 1_000));
            case "reset" -> Bindings.reset();
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

                  setup                                              exchange orders, notifications.email, audit.log and their bindings
                  publish  [--key order.created] [--count 1] [--first 1]
                  bind     --queue <name> --key <routing key>        declare the queue (if needed) and bind it to orders
                  unbind   --queue <name> --key <routing key>
                  consume  --queue <name> [--limit 0]                limit 0 = until Ctrl+C
                  status   [--samples 1] [--interval-ms 1000]        samples 0 = until Ctrl+C
                  reset                                              delete the exchange and the three queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key order.created"
                """);
    }
}
