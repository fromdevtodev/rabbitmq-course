package com.fromdevtodev.rabbitmq.topic;

import java.util.ArrayList;
import java.util.List;

/**
 * The names and the bindings of the story, and the model that predicts where a message goes ({@link TopicPattern}).
 *
 * <p>Two sets of bindings share the exchange {@code events}:
 * <ul>
 *   <li>{@link #STORY}: the four patterns of the episode (what audit and the other teams ask for);</li>
 *   <li>{@link #WILDCARDS}: three queues that isolate star, hash and a "wildcard inside a word" on the same keys.</li>
 * </ul>
 */
final class Rules {
    static final String EXCHANGE = "events";

    static final String CREATED_ORDERS = "created-orders";
    static final String ALL_ORDERS = "all-orders";
    static final String EU_AUDIT = "eu-audit";
    static final String PAYMENTS = "payments";

    static final String ORDERS_STAR = "orders-star";
    static final String ORDERS_HASH = "orders-hash";
    static final String ORDERS_GLOB = "orders-glob";

    /** One binding: "this queue wants the messages of the {@code events} exchange whose key matches this pattern". */
    record Binding(String queue, String pattern) { }

    /** The queues of {@link #STORY}, in the order they are shown. */
    static final List<String> STORY_QUEUES = List.of(CREATED_ORDERS, ALL_ORDERS, EU_AUDIT, PAYMENTS);

    /** created-orders: any region, created only. all-orders: every order event. eu-audit: any domain, region eu. */
    static final List<Binding> STORY = List.of(
            new Binding(CREATED_ORDERS, "orders.*.created"),
            new Binding(ALL_ORDERS, "orders.#"),
            new Binding(EU_AUDIT, "*.eu.*"),
            new Binding(PAYMENTS, "payments.#"));

    /** The queues of {@link #WILDCARDS}, in the order they are shown. */
    static final List<String> WILDCARD_QUEUES = List.of(ORDERS_STAR, ORDERS_HASH, ORDERS_GLOB);

    /** Star, hash, and a star inside a word (which is not a wildcard). */
    static final List<Binding> WILDCARDS = List.of(
            new Binding(ORDERS_STAR, "orders.*"),
            new Binding(ORDERS_HASH, "orders.#"),
            new Binding(ORDERS_GLOB, "ord*.eu.created"));

    private Rules() { }

    /** Every queue this project can create, so that {@code reset} leaves nothing behind. */
    static List<String> allQueues() {
        List<String> all = new ArrayList<>(STORY_QUEUES);
        all.addAll(WILDCARD_QUEUES);
        return all;
    }

    /** The queues, in binding order, that receive a copy of a message published with {@code routingKey}. */
    static List<String> queuesFor(List<Binding> bindings, String routingKey) {
        List<String> result = new ArrayList<>();
        for (Binding binding : bindings) {
            if (TopicPattern.matches(binding.pattern(), routingKey) && !result.contains(binding.queue())) {
                result.add(binding.queue());
            }
        }
        return result;
    }
}
