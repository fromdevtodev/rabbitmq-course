package com.fromdevtodev.rabbitmq.topic;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The rule of a topic exchange, written as code so that you can read it. RabbitMQ implements this rule itself: the project
 * never calls this class to route anything. It exists so that you can predict the result of a publish and then check your
 * prediction against the real broker ({@code ./run.sh app explain --key orders.eu.created}, then {@code ./run.sh experiment1}).
 */
final class TopicRule {
    private TopicRule() { }

    /** The queues that receive a copy: every queue with a binding whose pattern MATCHES the routing key. */
    static Set<String> queuesFor(String routingKey, List<Binding> bindings) {
        Set<String> queues = new LinkedHashSet<>();
        for (Binding binding : bindings) {
            if (matches(binding.pattern(), routingKey)) {
                queues.add(binding.queue());
            }
        }
        return queues;
    }

    // region readme:rule
    /** Does the pattern match the key? Both are lists of words separated by dots. */
    static boolean matches(String pattern, String key) {
        return matches(pattern.split("\\."), 0, key.split("\\."), 0);
    }

    private static boolean matches(String[] pattern, int p, String[] key, int k) {
        if (p == pattern.length) {
            return k == key.length;                      // the pattern is used up: the key must be used up too
        }
        if (pattern[p].equals("#")) {                    // hash: zero or more words
            for (int skip = k; skip <= key.length; skip++) {
                if (matches(pattern, p + 1, key, skip)) {
                    return true;
                }
            }
            return false;
        }
        if (k == key.length) {
            return false;                                // the key has no word left for this part of the pattern
        }
        boolean sameWord = pattern[p].equals("*") || pattern[p].equals(key[k]);   // star: exactly one word
        return sameWord && matches(pattern, p + 1, key, k + 1);
    }
    // endregion
}
