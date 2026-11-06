package com.fromdevtodev.rabbitmq.fanout;

import java.util.ArrayList;
import java.util.List;

/**
 * The bindings of the story, and our own model of the one rule a fanout exchange follows: a message goes to <em>every</em>
 * queue bound to the exchange, and the routing key plays no part.
 *
 * <p>This is a model for predicting and for the tests; it uses no broker. The broker is the source of truth, and the
 * experiments compare the two.
 */
final class Rules {
    static final String EXCHANGE = "price-updates";

    /** One cache per service instance. In the video there are 12 instances; three are enough to see the rule. */
    static final String INSTANCE_1 = "cache.instance-1";
    static final String INSTANCE_2 = "cache.instance-2";
    static final String INSTANCE_3 = "cache.instance-3";
    /** One queue that several consumers share. It is not part of {@link #QUEUES}; {@code setup-shared} creates it. */
    static final String SHARED = "cache.shared";

    /** One binding: "this queue wants the messages of the {@code price-updates} exchange". The key is on the binding. */
    record Binding(String queue, String key) { }

    /** The queues of the {@code price-updates} exchange, in the order they are shown. */
    static final List<String> QUEUES = List.of(INSTANCE_1, INSTANCE_2, INSTANCE_3);

    /**
     * Three different binding keys on purpose: an empty one, {@code books} and {@code electronics}. A direct exchange
     * would care which key each message carries. A fanout exchange does not.
     */
    static final List<Binding> BINDINGS = List.of(
            new Binding(INSTANCE_1, ""),
            new Binding(INSTANCE_2, "books"),
            new Binding(INSTANCE_3, "electronics"));

    private Rules() { }

    /** The queues that receive a copy of a message published with {@code routingKey}, given the standard bindings. */
    static List<String> queuesFor(String routingKey) {
        return queuesFor(routingKey, BINDINGS);
    }

    /**
     * Every queue that has a binding to the exchange, once, in binding order. The {@code routingKey} parameter is
     * deliberately not used: that is the rule. With no bindings, the result is empty and the message is not delivered.
     */
    static List<String> queuesFor(String routingKey, List<Binding> bindings) {
        List<String> result = new ArrayList<>();
        for (Binding binding : bindings) {
            if (!result.contains(binding.queue())) {
                result.add(binding.queue());
            }
        }
        return result;
    }
}
