package com.fromdevtodev.rabbitmq.manualack;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                       declare the queue notifications.email
 * publish [--count 1] [--first 1]             the order service: publish order events and go away
 * work [--name email-worker] [options]        the email worker: automatic or manual acknowledgement, with an optional crash
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
            case "work" -> EmailWorker.run(
                    options.text("name", "email-worker"),
                    AckMode.parse(options.text("mode", "manual")),
                    CrashPoint.parse(options.text("crash", "none")),
                    options.number("send-ms", 3_000),
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
                  work     [--name email-worker]               the email worker; the name shows up in the management UI
                           [--mode manual]                     auto: the broker forgets a message when it sends it
                                                               manual: the worker acknowledges after sending the email
                           [--crash none]                      none | while-sending | after-send: where the worker dies
                           [--send-ms 3000]                    how long sending one email takes
                           [--limit 0]                         stop after N messages (0 = until Ctrl+C)
                  reset                                        delete the queue

                With Maven:  mvn -q compile exec:java -Dexec.args="work --mode manual --crash after-send"
                """);
    }
}
