package com.fromdevtodev.rabbitmq.direct;

import java.util.Arrays;
import java.util.List;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                            exchange logs, queues errors-pager, audit, metrics, and their five bindings
 * publish          --key error [--count 1] [--first 1]      publish to the exchange "logs" with a routing key
 * predict          --key warning                            which queues should get a copy? (our model; the broker decides)
 * publish-default  --queue notifications.email [--count 1]  publish to the default exchange ("") using a queue name as key
 * declare-queue    --queue notifications.email              declare a queue and nothing else
 * try-bind-default --queue notifications.email [--key x]    try to bind a queue to the default exchange yourself
 * consume          --queue audit [--limit 0]                take messages from one queue and print them
 * status           [--queues a,b] [--samples 1]             print "ready" for each queue
 * reset                                                     delete the exchange and the queues
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
                    options.required("key"), options.number("count", 1), options.number("first", 1));
            case "predict" -> predict(options.required("key"));
            case "publish-default" -> Publisher.toDefaultExchange(
                    options.required("queue"), options.number("count", 1), options.number("first", 1));
            case "declare-queue" -> Bindings.declareQueue(options.required("queue"));
            case "try-bind-default" -> Bindings.tryBindToDefaultExchange(
                    options.required("queue"), options.text("key", "x"));
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
                ? "key '" + key + "': no binding matches, so no queue receives a copy"
                : "key '" + key + "': copies go to " + String.join(", ", queues));
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                              exchange logs, three queues, five bindings
                  publish          --key <routing key> [--count 1] [--first 1]
                  predict          --key <routing key>               our model of the rule; the broker has the last word
                  publish-default  --queue <name> [--count 1] [--first 1]
                  declare-queue    --queue <name>                    declare a queue and nothing else
                  try-bind-default --queue <name> [--key x]          the broker refuses to bind to the default exchange
                  consume          --queue <name> [--limit 0]        limit 0 = until Ctrl+C
                  status           [--queues a,b,c] [--samples 1] [--interval-ms 1000]
                  reset                                              delete the exchange and the queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key error"
                """);
    }
}
