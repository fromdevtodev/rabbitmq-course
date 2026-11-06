package com.fromdevtodev.rabbitmq.ack;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                       declare the queue notifications.email
 * publish [--count 1] [--first 1]             publish order events to the queue (default exchange)
 * work    --mode auto|manual [options]        an email worker; see below
 * reset                                       delete the queue
 * </pre>
 *
 * <p>Options of {@code work}: {@code --name} (connection name, default email-worker), {@code --crash} (none,
 * while-sending, after-send; default none), {@code --send-ms} (how long "sending" takes, default 4000), {@code --limit}
 * (stop after N messages, default 0 = until Ctrl+C).
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
            case "setup" -> Setup.run();
            case "publish" -> Publisher.publish(options.number("count", 1), options.number("first", 1));
            case "work" -> EmailWorker.run(
                    options.text("name", "email-worker"),
                    AckMode.parse(options.required("mode")),
                    CrashPoint.parse(options.text("crash", "none")),
                    options.number("send-ms", 4_000),
                    options.number("limit", 0));
            case "reset" -> Topology.reset();
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
                  publish  [--count 1] [--first 1]             publish order events (evt_0001, ...)
                  work     --mode auto|manual                  an email worker
                           [--name email-worker]               connection name, shown in the management UI
                           [--crash none|while-sending|after-send]
                           [--send-ms 4000]                    how long "sending the email" takes
                           [--limit 0]                         stop after N messages (0 = until Ctrl+C)
                  reset                                        delete the queue

                With Maven:  mvn -q compile exec:java -Dexec.args="publish"
                """);
    }
}
