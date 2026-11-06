package com.fromdevtodev.rabbitmq.direct;

import java.util.ArrayList;
import java.util.List;

/**
 * The bindings of the story, and our own model of the one rule a direct exchange follows: a message goes to every queue
 * that has a binding whose key is <em>exactly equal</em> to the message's routing key.
 *
 * <p>This is a model for predicting and for the tests; it uses no broker. The broker is the source of truth, and the
 * experiments compare the two.
 */
final class Rules {
    static final String EXCHANGE = "logs";

    static final String ERRORS_PAGER = "errors-pager";
    static final String AUDIT = "audit";
    static final String METRICS = "metrics";
    /** The queue used for the default-exchange demo. It is not bound to {@code logs}. */
    static final String EMAIL = "notifications.email";

    static final String ERROR = "error";
    static final String WARNING = "warning";
    static final String INFO = "info";
    static final String DEBUG = "debug";

    /** One binding: "this queue wants the messages of the {@code logs} exchange that carry this key". */
    record Binding(String queue, String key) { }

    /** The queues of the {@code logs} exchange, in the order they are shown. */
    static final List<String> QUEUES = List.of(ERRORS_PAGER, AUDIT, METRICS);

    /** errors-pager gets only errors; audit keeps info, warning and error; metrics counts warnings. */
    static final List<Binding> BINDINGS = List.of(
            new Binding(ERRORS_PAGER, ERROR),
            new Binding(AUDIT, INFO),
            new Binding(AUDIT, WARNING),
            new Binding(AUDIT, ERROR),
            new Binding(METRICS, WARNING));

    private Rules() { }

    /**
     * The queues that receive a copy of a message published with {@code routingKey}: one entry per queue, even when a
     * queue is bound with several keys, because a message carries a single key.
     */
    static List<String> queuesFor(String routingKey) {
        List<String> result = new ArrayList<>();
        for (Binding binding : BINDINGS) {
            if (binding.key().equals(routingKey) && !result.contains(binding.queue())) {
                result.add(binding.queue());
            }
        }
        return result;
    }
}
