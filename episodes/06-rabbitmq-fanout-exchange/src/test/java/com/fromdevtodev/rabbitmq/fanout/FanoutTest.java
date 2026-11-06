package com.fromdevtodev.rabbitmq.fanout;

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
 * Executable versions of the claims in the Fanout Exchange episode. Routing is predicted by
 * {@link Rules#queuesFor}; the routing key is ignored on purpose.
 */
class FanoutTest {

    @Test
    void everyBoundQueueReceivesACopyRegardlessOfRoutingKey() {
        List<String> expected = List.of(Rules.INSTANCE_1, Rules.INSTANCE_2, Rules.INSTANCE_3);
        assertEquals(expected, Rules.queuesFor("books"));
        assertEquals(expected, Rules.queuesFor("electronics"));
        assertEquals(expected, Rules.queuesFor(""));
        assertEquals(expected, Rules.queuesFor("anything-at-all"));
    }

    @Test
    void differentBindingKeysDoNotFilterFanout() {
        // BINDINGS deliberately use "", "books" and "electronics"; a direct exchange would care.
        assertEquals(3, Rules.BINDINGS.size());
        assertEquals(Rules.queuesFor("books"), Rules.queuesFor("electronics"));
    }

    @Test
    void withNoBindingsNothingReceivesTheMessage() {
        assertTrue(Rules.queuesFor("books", List.of()).isEmpty());
    }

    @Test
    void sharedQueueIsNotPartOfThePerInstanceStory() {
        assertFalse(Rules.QUEUES.contains(Rules.SHARED),
                "cache.shared is created only by setup-shared");
    }

    @Test
    void messageBodySurvivesAJsonRoundTrip() throws Exception {
        PriceChange original = PriceChange.of(4, "books");
        assertEquals(original, EventJson.fromBytes(EventJson.toBytes(original)));
    }

    @Test
    void statusLineShowsEveryQueueAndMarksMissingOnes() {
        Map<String, Long> ready = new LinkedHashMap<>();
        ready.put(Rules.INSTANCE_1, 2L);
        ready.put(Rules.INSTANCE_2, null);
        ready.put(Rules.INSTANCE_3, 0L);
        assertEquals(
                "14:03:07 | cache.instance-1 ready 2 | cache.instance-2 does not exist yet | cache.instance-3 ready 0",
                StatusLine.format(LocalTime.of(14, 3, 7, 999_000_000), ready));
    }

    @Test
    void argsParseNumbersTextAndRequiredOptions() {
        Args args = new Args(new String[] {"publish", "--category", "books", "--count", "5"}, 1);
        assertEquals("books", args.required("category"));
        assertEquals(5L, args.number("count", 1));
        assertThrows(IllegalArgumentException.class, () -> args.required("missing"));
    }

    @Test
    void argsRejectMalformedInput() {
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"publish", "category", "books"}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"publish", "--category"}, 1));
    }
}
