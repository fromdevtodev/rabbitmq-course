package com.fromdevtodev.rabbitmq.fanout;

import java.util.Arrays;
import java.util.List;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup                                           exchange price-updates, queues cache.instance-1..3 and their three bindings
 * setup-shared                                    queue cache.shared, bound to the exchange (for the shared-queue experiment)
 * publish    --key books [--count 1] [--first 1]  publish to the exchange "price-updates" with a routing key
 * predict    --key books                          which queues should get a copy? (our model; the broker decides)
 * consume    --queue cache.shared [--name a]      a consumer of one named queue (start it twice: two consumers, one queue)
 * subscribe  [--name instance-1] [--limit 0]      one instance with its own exclusive queue, bound to the exchange
 * status     [--queues a,b] [--samples 1]         print "ready" for each named queue
 * reset                                           delete the exchange and the queues
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
            case "setup-shared" -> Bindings.setupShared();
            case "publish" -> Publisher.toExchange(
                    options.required("key"), options.number("count", 1), options.number("first", 1));
            case "predict" -> predict(options.required("key"));
            case "consume" -> {
                String queue = options.required("queue");
                Consumer.run(queue, options.text("name", queue), options.number("limit", 0));
            }
            case "subscribe" -> Subscriber.run(options.text("name", "instance"), options.number("limit", 0));
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
                ? "key '" + key + "': no queue is bound, so no queue receives a copy"
                : "key '" + key + "': copies go to " + String.join(", ", queues) + " (the key is ignored)");
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup                                              exchange price-updates, three queues, three bindings
                  setup-shared                                       queue cache.shared, bound to the exchange
                  publish    --key <routing key> [--count 1] [--first 1]
                  predict    --key <routing key>                     our model of the rule; the broker has the last word
                  consume    --queue <name> [--name <label>] [--limit 0]     limit 0 = until Ctrl+C
                  subscribe  [--name <label>] [--limit 0]            an exclusive queue of its own, bound to the exchange
                  status     [--queues a,b,c] [--samples 1] [--interval-ms 1000]
                  reset                                              delete the exchange and the queues

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key books"
                """);
    }
}
