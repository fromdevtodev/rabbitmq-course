package com.fromdevtodev.rabbitmq.topic;

/**
 * Our own model of the rule a topic exchange follows. The broker is the source of truth; this class exists to predict,
 * to teach and to test without a broker, and the experiments compare the two.
 *
 * <p>A routing key is a list of <em>words</em> separated by dots ({@code orders.eu.created}: three words). A binding
 * pattern is written the same way, and a word of the pattern can be:
 *
 * <ul>
 *   <li>a plain word: matches that word and nothing else;</li>
 *   <li><b>{@code *}</b> (star): matches exactly one word;</li>
 *   <li><b>{@code #}</b> (hash): matches zero or more words.</li>
 * </ul>
 *
 * <p>Wildcards are whole words. {@code ord*} is not a wildcard: it is a plain word with a star in it, and it matches
 * only a key word that is literally {@code ord*}. Keys are expected to be non-empty words joined by single dots.
 */
final class TopicPattern {
    private TopicPattern() { }

    /** True if a message published with {@code routingKey} is routed by a binding made with {@code pattern}. */
    static boolean matches(String pattern, String routingKey) {
        return match(words(pattern), 0, words(routingKey), 0);
    }

    static String[] words(String key) {
        return key.split("\\.", -1);
    }

    private static boolean match(String[] pattern, int p, String[] key, int k) {
        if (p == pattern.length) {
            return k == key.length;
        }
        String word = pattern[p];
        if (word.equals("#")) {
            // Zero or more words: try every possible length of the part of the key that the hash swallows.
            for (int next = k; next <= key.length; next++) {
                if (match(pattern, p + 1, key, next)) {
                    return true;
                }
            }
            return false;
        }
        if (k == key.length) {
            return false;
        }
        if (word.equals("*") || word.equals(key[k])) {
            return match(pattern, p + 1, key, k + 1);
        }
        return false;
    }
}
