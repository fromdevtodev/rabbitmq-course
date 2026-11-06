package com.fromdevtodev.rabbitmq.topic;

import java.util.Arrays;
import java.util.List;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup            [--set story|wildcards]                  exchange events, its queues and their bindings
 * bind             --queue Q --pattern P                    one more binding (declares the queue if needed)
 * publish          --key orders.eu.created [--count 1] [--first 1]   publish to the exchange "events" with a routing key
 * match            --pattern P --key K                      does the pattern match the key? (our model, no broker)
 * predict          --key K [--set story|wildcards]          which queues should get a copy? (our model; the broker decides)
 * consume          --queue Q [--limit 0]                    take messages from one queue and print them
 * status           [--queues a,b] [--samples 1]             print "ready" for each queue
 * reset            [--also a,b]                           delete the exchange and every queue of the project (and extra queues)
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
            case "setup" -> Bindings.setup(bindingSet(options.text("set", "story")));
            case "bind" -> Bindings.bind(options.required("queue"), options.required("pattern"));
            case "publish" -> Publisher.run(
                    options.required("key"), options.number("count", 1), options.number("first", 1));
            case "match" -> match(options.required("pattern"), options.required("key"));
            case "predict" -> predict(options.required("key"), bindingSet(options.text("set", "story")));
            case "consume" -> Consumer.run(options.required("queue"), options.number("limit", 0));
            case "status" -> QueueStatus.run(
                    queuesOf(options.text("queues", String.join(",", Rules.STORY_QUEUES))),
                    options.number("samples", 1), options.number("interval-ms", 1_000));
            case "reset" -> Bindings.reset(queuesOf(options.text("also", "")));
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static List<Rules.Binding> bindingSet(String name) {
        return switch (name) {
            case "story" -> Rules.STORY;
            case "wildcards" -> Rules.WILDCARDS;
            default -> throw new IllegalArgumentException("--set must be 'story' or 'wildcards', got: " + name);
        };
    }

    private static List<String> queuesOf(String commaSeparated) {
        return Arrays.stream(commaSeparated.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    private static void match(String pattern, String key) {
        System.out.println("pattern '" + pattern + "' " + (TopicPattern.matches(pattern, key) ? "matches" : "does not match")
                + " key '" + key + "'");
    }

    private static void predict(String key, List<Rules.Binding> bindings) {
        List<String> queues = Rules.queuesFor(bindings, key);
        System.out.println(queues.isEmpty()
                ? "key '" + key + "': no pattern matches, so no queue receives a copy"
                : "key '" + key + "': copies go to " + String.join(", ", queues));
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup    [--set story|wildcards]                   exchange events, queues and bindings (default: story)
                  bind     --queue <name> --pattern <pattern>        one more binding
                  publish  --key <routing key> [--count 1] [--first 1]
                  match    --pattern <pattern> --key <key>           our model of the rule; no broker needed
                  predict  --key <key> [--set story|wildcards]       which queues should get a copy (the broker has the last word)
                  consume  --queue <name> [--limit 0]                limit 0 = until Ctrl+C
                  status   [--queues a,b,c] [--samples 1] [--interval-ms 1000]
                  reset    [--also a,b]                              delete the exchange, every project queue and the extra queues you bound

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --key orders.eu.created"
                """);
    }
}
