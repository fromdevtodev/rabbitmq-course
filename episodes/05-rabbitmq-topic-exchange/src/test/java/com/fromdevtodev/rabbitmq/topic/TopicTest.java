package com.fromdevtodev.rabbitmq.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Executable versions of the claims in the Topic Exchange episode. Routing is predicted by
 * {@link TopicPattern} and {@link Rules#queuesFor}; no broker is required.
 */
class TopicTest {

    @Test
    void starMatchesExactlyOneWord() {
        assertTrue(TopicPattern.matches("orders.*.created", "orders.eu.created"));
        assertTrue(TopicPattern.matches("orders.*.created", "orders.us.created"));
        assertFalse(TopicPattern.matches("orders.*.created", "orders.eu.uk.created"),
                "star does not span two words");
        assertFalse(TopicPattern.matches("orders.*.created", "orders.created"),
                "star needs exactly one word");
    }

    @Test
    void hashMatchesZeroOrMoreWords() {
        assertTrue(TopicPattern.matches("orders.#", "orders"));
        assertTrue(TopicPattern.matches("orders.#", "orders.eu.created"));
        assertTrue(TopicPattern.matches("orders.#", "orders.eu.uk.created"));
        assertFalse(TopicPattern.matches("orders.#", "payments.eu.created"));
    }

    @Test
    void starInsideAWordIsNotAWildcard() {
        assertTrue(TopicPattern.matches("ord*.eu.created", "ord*.eu.created"));
        assertFalse(TopicPattern.matches("ord*.eu.created", "orders.eu.created"),
                "ord* is a literal word, not a prefix match");
    }

    @Test
    void storyBindingsRouteAsDocumented() {
        assertEquals(List.of(Rules.CREATED_ORDERS, Rules.ALL_ORDERS, Rules.EU_AUDIT),
                Rules.queuesFor(Rules.STORY, "orders.eu.created"));
        assertEquals(List.of(Rules.ALL_ORDERS),
                Rules.queuesFor(Rules.STORY, "orders.us.shipped"));
        assertEquals(List.of(Rules.EU_AUDIT),
                Rules.queuesFor(Rules.STORY, "inventory.eu.low"));
        assertEquals(List.of(Rules.PAYMENTS),
                Rules.queuesFor(Rules.STORY, "payments.card.captured"));
        assertTrue(Rules.queuesFor(Rules.STORY, "user.signup").isEmpty());
    }

    @Test
    void wildcardDemoIsolatesStarHashAndLiteralStar() {
        // orders.* matches exactly one word after "orders" → orders.eu (two words total)
        assertEquals(List.of(Rules.ORDERS_STAR, Rules.ORDERS_HASH),
                Rules.queuesFor(Rules.WILDCARDS, "orders.eu"));

        // orders.eu.created has three words → only the hash binding matches
        assertEquals(List.of(Rules.ORDERS_HASH),
                Rules.queuesFor(Rules.WILDCARDS, "orders.eu.created"));
        assertFalse(Rules.queuesFor(Rules.WILDCARDS, "orders.eu.created")
                .contains(Rules.ORDERS_STAR));

        // ord* is a literal word, not a prefix wildcard
        assertTrue(Rules.queuesFor(Rules.WILDCARDS, "ord*.eu.created")
                .contains(Rules.ORDERS_GLOB));
        assertFalse(Rules.queuesFor(Rules.WILDCARDS, "orders.eu.created")
                .contains(Rules.ORDERS_GLOB));
    }

    @Test
    void messageBodySurvivesAJsonRoundTrip() throws Exception {
        Event original = Event.of(3, "orders.eu.created");
        assertEquals(original, EventJson.fromBytes(EventJson.toBytes(original)));
    }

    @Test
    void statusLineShowsEveryQueueAndMarksMissingOnes() {
        Map<String, Long> ready = new LinkedHashMap<>();
        ready.put(Rules.CREATED_ORDERS, 2L);
        ready.put(Rules.ALL_ORDERS, null);
        ready.put(Rules.EU_AUDIT, 0L);
        assertEquals(
                "14:03:07 | created-orders ready 2 | all-orders does not exist yet | eu-audit ready 0",
                StatusLine.format(LocalTime.of(14, 3, 7, 999_000_000), ready));
    }

    @Test
    void argsParseNumbersTextAndRequiredOptions() {
        Args args = new Args(new String[] {"publish", "--key", "orders.eu.created", "--count", "5"}, 1);
        assertEquals("orders.eu.created", args.required("key"));
        assertEquals(5L, args.number("count", 1));
        assertEquals(1L, args.number("first", 1));
        assertThrows(IllegalArgumentException.class, () -> args.required("missing"));
    }

    @Test
    void argsRejectMalformedInput() {
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"publish", "key", "orders.eu.created"}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"publish", "--key"}, 1));
    }
}
