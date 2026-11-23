package com.fromdevtodev.rabbitmq.prefetch;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                       declare the queue notifications.email
 * publish [--count 1] [--first 1]             the producer: publish order events and go away
 * work --name fast|slow [options]             an email worker: take messages, handle them, acknowledge them
 * reset                                       delete the queue
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
            case "publish" -> Publisher.publish(options.number("count", 1), options.number("first", 1));
            case "work" -> Worker.run(
                    options.text("name", "worker"),
                    options.number("delay-ms", 500),
                    (int) options.number("prefetch", 0),
                    options.number("limit", 0));
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

                  setup                                        declare the queue notifications.email
                  publish  [--count 1] [--first 1]             publish order events (evt_0001, ...) and exit
                  work     [--name worker]                     an email worker; the name shows up in the management UI
                           [--delay-ms 500]                    how long "handling" one message takes (50 = fast, 500 = slow)
                           [--prefetch 0]                      unacknowledged messages allowed at once (0 = no limit)
                           [--limit 0]                         stop after N messages (0 = until Ctrl+C)
                  reset                                        delete the queue

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --count 5"
                """);
    }
}
