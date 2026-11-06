package com.fromdevtodev.rabbitmq.unroutable;

import java.util.ArrayList;
import java.util.List;

/**
 * The bindings of the story, and our own model of the rule a direct exchange follows: a message goes to every queue
 * that has a binding whose key is <em>exactly equal</em> to the message's routing key. A message that matches no
 * binding is <em>unroutable</em>.
 *
 * <p>This is a model for predicting and for the tests; it uses no broker. The broker is the source of truth, and the
 * experiments compare the two.
 */
final class Rules {
    static final String EXCHANGE = "orders";

    static final String EMAIL = "notifications.email";
    static final String POINTS = "loyalty.points";
    static final String AUDIT = "audit.log";

    static final String CREATED = "order.created";
    static final String CANCELLED = "order.cancelled";
    /** The typo of the story: one L. No binding has this key. */
    static final String CANCELED = "order.canceled";

    /** The alternate exchange and the queue behind it (episode section "alternate exchange"). */
    static final String ALTERNATE_EXCHANGE = "orders.unrouted";
    static final String PARKING_QUEUE = "unrouted.parking";

    /** One binding: "this queue wants the messages of the {@code orders} exchange that carry this key". */
    record Binding(String queue, String key) { }

    /** The queues of the {@code orders} exchange, in the order they are shown. */
    static final List<String> QUEUES = List.of(EMAIL, POINTS, AUDIT);

    static final List<Binding> BINDINGS = List.of(
            new Binding(EMAIL, CREATED),
            new Binding(EMAIL, CANCELLED),
            new Binding(POINTS, CREATED),
            new Binding(AUDIT, CREATED),
            new Binding(AUDIT, CANCELLED));

    private Rules() { }

    /** The queues that receive a copy of a message published with {@code routingKey}; empty means unroutable. */
    static List<String> queuesFor(String routingKey) {
        List<String> result = new ArrayList<>();
        for (Binding binding : BINDINGS) {
            if (binding.key().equals(routingKey) && !result.contains(binding.queue())) {
                result.add(binding.queue());
            }
        }
        return result;
    }

    static boolean isUnroutable(String routingKey) {
        return queuesFor(routingKey).isEmpty();
    }
}
