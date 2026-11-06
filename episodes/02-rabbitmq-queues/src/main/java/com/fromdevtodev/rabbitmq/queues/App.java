package com.fromdevtodev.rabbitmq.queues;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * publish   --count N [--rate R] [--first 1]     put N numbered messages at the tail of the queue
 * consume   [--rate R] [--prefetch 1] [--log-every 1]   take messages from the head, R per second
 * status    [--samples 0] [--interval-ms 1000]    print "ready" and "consumers" for the queue
 * reset                                           delete the queue and its messages
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
            case "publish" -> Publisher.run(
                    options.number("count", 4), options.number("first", 1), options.decimal("rate", 0));
            case "consume" -> Consumer.run(
                    options.decimal("rate", 1), (int) options.number("prefetch", 1), options.number("log-every", 1));
            case "status" -> QueueStatus.run(options.number("samples", 1), options.number("interval-ms", 1_000));
            case "reset" -> QueueStatus.delete();
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

                  publish   --count 4 [--rate 0] [--first 1]       rate 0 = as fast as possible
                  consume   [--rate 1] [--prefetch 1] [--log-every 1]
                  status    [--samples 1] [--interval-ms 1000]     samples 0 = until Ctrl+C
                  reset                                            delete the queue and its messages

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --count 4"
                """);
    }
}
