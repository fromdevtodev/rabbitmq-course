package com.fromdevtodev.rabbitmq.confirms;

import java.nio.file.Path;

/**
 * Entry point. Run each command in its own terminal:
 *
 * <pre>
 * setup   [--limit 2]                                     declare orders.in (durable, classic); --limit none: no length limit
 * publish [--count 1] [--first 1]                         publish order events with publisher confirms and log every answer
 *         [--pace one-by-one] [--timeout-ms 5000]         (pace burst: publish all, then collect the answers)
 * republish [--timeout-ms 3000]                         evt_0001, then evt_0002 with no answer, then evt_0002 again
 * (both)    [--gate path]                                hold each message until a script creates this file (see run.sh)
 * explain [--heard confirm]                               what an answer lets the producer conclude (no broker needed)
 * reset                                                   delete the queue
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
            case "setup" -> Setup.run(limit(options.text("limit", "2")));
            case "publish" -> Publisher.publish(
                    options.number("count", 1),
                    options.number("first", 1),
                    pace(options.text("pace", "one-by-one")),
                    options.number("timeout-ms", 5_000),
                    gate(options));
            case "republish" -> Republish.run(
                    gate(options),
                    options.number("timeout-ms", 3_000),
                    options.number("settle-ms", 60_000));
            case "explain" -> explain(heard(options.text("heard", "confirm")));
            case "reset" -> Topology.reset();
            // endregion
            default -> {
                System.err.println("Unknown command: " + args[0]);
                usage();
                System.exit(2);
            }
        }
    }

    private static Gate gate(Args options) {
        return options.has("gate") ? new Gate(Path.of(options.text("gate", ""))) : Gate.open();
    }

    private static Integer limit(String text) {
        return text.equalsIgnoreCase("none") ? null : Integer.valueOf(text);
    }

    private static Publisher.Pace pace(String text) {
        return switch (text) {
            case "one-by-one" -> Publisher.Pace.ONE_BY_ONE;
            case "burst" -> Publisher.Pace.BURST;
            default -> throw new IllegalArgumentException("--pace must be one-by-one or burst, not " + text);
        };
    }

    private static ConfirmRule.Heard heard(String text) {
        return switch (text) {
            case "confirm" -> ConfirmRule.Heard.CONFIRM;
            case "nack" -> ConfirmRule.Heard.NACK;
            case "silence" -> ConfirmRule.Heard.SILENCE;
            default -> throw new IllegalArgumentException("--heard must be confirm, nack or silence, not " + text);
        };
    }

    private static void explain(ConfirmRule.Heard heard) {
        ConfirmRule.Verdict verdict = ConfirmRule.whatYouKnow(heard);
        System.out.println("heard: " + heard.name().toLowerCase() + " -> " + verdict.knowledge);
        System.out.println("next move: " + verdict.nextMove);
    }

    private static void usage() {
        System.out.println("""
                Usage: App <command> [--option value ...]

                  setup      [--limit 2]                          declare orders.in (durable, classic); with a limit, a full queue
                                                                  rejects the next publish. --limit none: no limit
                  publish    [--count 1] [--first 1]              how many events, and the number of the first (evt_0001, ...)
                             [--pace one-by-one]                  one-by-one: wait for each answer; burst: publish all, then wait
                             [--timeout-ms 5000]                  how long to wait for one answer
                  republish  [--timeout-ms 3000]                  evt_0001, then evt_0002 with no answer, then evt_0002 again
                  publish and republish also take [--gate path]: hold each message until a script creates that file (see run.sh)
                  explain    [--heard confirm]                    confirm, nack or silence: what the producer may conclude
                  reset                                           delete the queue

                With Maven:  mvn -q compile exec:java -Dexec.args="publish --count 3"
                """);
    }
}
