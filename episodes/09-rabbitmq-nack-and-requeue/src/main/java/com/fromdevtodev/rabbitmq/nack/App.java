package com.fromdevtodev.rabbitmq.nack;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                                   exchange orders, queue notifications.email, one binding
 * publish  [--first 1] [--count 3] [--poison 2]           publish order.created events; number 2 has no email address
 * consume  [--on-failure requeue|discard|by-kind]         the email worker: one message at a time, ack or nack
 *          [--work-ms 250] [--flaky 0] [--limit 0]
 * reset                                                   delete the exchange and the queue
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
            case "publish" -> Publisher.toExchange(
                    options.number("first", 1), options.number("count", 3), options.number("poison", 2));
            case "consume" -> Consumer.run(
                    Rules.OnFailure.parse(options.text("on-failure", "requeue")),
                    options.number("work-ms", 250), (int) options.number("flaky", 0), options.number("limit", 0));
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

                  setup                                            exchange orders, queue notifications.email, one binding
                  publish  [--first 1] [--count 3] [--poison 2]    --poison N: message number N has no email address (0: none)
                  consume  [--on-failure requeue|discard|by-kind]  what the worker asks for when it cannot process a message
                           [--work-ms 250]                         time the "email" takes, so the log is readable
                           [--flaky 0]                             the first N attempts of every event fail with a transient error
                           [--limit 0]                             stop after N answered deliveries (0 = until Ctrl+C)
                  reset                                            delete the exchange and the queue

                With Maven:  mvn -q compile exec:java -Dexec.args="consume --on-failure discard"
                """);
    }
}
