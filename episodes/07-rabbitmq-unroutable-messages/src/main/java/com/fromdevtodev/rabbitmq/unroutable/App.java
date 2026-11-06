package com.fromdevtodev.rabbitmq.unroutable;

import java.util.Arrays;
import java.util.List;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                                exchange orders, queues notifications.email, loyalty.points, audit.log, five bindings
 * setup-parking                                        exchange orders.unrouted (fanout) and queue unrouted.parking, not attached yet
 * publish   --key order.canceled [--mandatory true] [--count 1] [--first 1]
 * predict   --key order.canceled                       which queues should get a copy? (our model; the broker decides)
 * consume   --queue unrouted.parking [--limit 0]       take messages from one queue and print them
 * status    [--queues a,b] [--samples 1]               print "ready" for each queue
 * reset                                                delete both exchanges and the queues
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
            case "setup-parking" -> Bindings.setupParking();
            case "publish" -> Publisher.toExchange(options.required("key"),
                    Boolean.parseBoolean(options.text("mandatory", "false")),
                    options.number("count", 1), options.number("first", 1));
            case "predict" -> predict(options.required("key"));
            case "consume" -> Consumer.run(options.required("queue"), options.number("limit", 0));
            case "status" -> QueueStatus.run(
                    queuesOf(options.text("queues", String.join(",", Rules.QUEUES))),
                    options.number("samples", 1), options.number("interval-ms", 1_000));
            case "reset" -> Bindings.reset();
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static List<String> queuesOf(String commaSeparated) {
        return Arrays.stream(commaSeparated.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    private static void predict(String key) {
        List<String> queues = Rules.queuesFor(key);
        System.out.println(queues.isEmpty()
                ? "key '" + key + "': no binding matches, so the message is unroutable"
                : "key '" + key + "': copies go to " + String.join(", ", queues));
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                            exchange orders, three queues, five bindings
                  setup-parking                                    exchange orders.unrouted and queue unrouted.parking
                  publish  --key <routing key> [--mandatory true] [--count 1] [--first 1]
                  predict  --key <routing key>                     our model of the rule; the broker has the last word
                  consume  --queue <n> [--limit 0]                 limit 0 = until Ctrl+C
                  status   [--queues a,b,c] [--samples 1] [--interval-ms 1000]
                  reset                                            delete both exchanges and the queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key order.canceled --mandatory true"
                """);
    }
}
