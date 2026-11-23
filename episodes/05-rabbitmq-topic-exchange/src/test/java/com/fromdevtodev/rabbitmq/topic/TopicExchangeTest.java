package com.fromdevtodev.rabbitmq.topic;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the rule and the small pieces of the project that do not talk to RabbitMQ. */
class TopicExchangeTest {

    // region readme:test
    @Test
    void everyQueueWithAMatchingPatternGetsACopy() {
        assertEquals(Set.of("created-orders", "all-orders", "eu-audit"), TopicRule.queuesFor("orders.eu.created", Topology.ROUTING));
        assertEquals(Set.of("all-orders"), TopicRule.queuesFor("orders.us.cancelled", Topology.ROUTING));
        assertEquals(Set.of("eu-audit", "payments"), TopicRule.queuesFor("payments.eu.failed", Topology.ROUTING));
    }

    @Test
    void starIsExactlyOneWordAndHashIsZeroOrMore() {
        assertFalse(TopicRule.matches("orders.*", "orders"));
        assertTrue(TopicRule.matches("orders.*", "orders.eu"));
        assertFalse(TopicRule.matches("orders.*", "orders.eu.created"));

        assertTrue(TopicRule.matches("orders.#", "orders"));
        assertTrue(TopicRule.matches("orders.#", "orders.eu"));
        assertTrue(TopicRule.matches("orders.#", "orders.eu.created"));
    }

    @Test
    void aStarInsideAWordIsNotAWildcard() {
        assertFalse(TopicRule.matches("ord*.eu.created", "orders.eu.created"));
        assertFalse(TopicRule.matches("ord*", "orders"));
    }

    @Test
    void aKeyThatMatchesNoPatternDeliversNothing() {
        assertTrue(TopicRule.queuesFor("shipments.us.created", Topology.ROUTING).isEmpty());
    }
    // endregion

    // region readme:test-audit
    @Test
    void directBindingsGrowWithEveryRegionButTwoPatternsStayTheSame() {
        List<String> twoRegions = auditKeys("eu", "us");
        List<String> threeRegions = auditKeys("eu", "us", "apac");
        assertEquals(6, twoRegions.size());     // one direct binding per key
        assertEquals(8, threeRegions.size());
        for (String key : threeRegions) {
            boolean covered = TopicRule.matches("payments.#", key) || TopicRule.matches("orders.eu.*", key);
            assertTrue(covered, key);           // the same two patterns still cover every key
        }
    }

    /** What audit wants: every payment event, whatever the region, and the EU orders. */
    private static List<String> auditKeys(String... regions) {
        List<String> keys = new ArrayList<>();
        for (String region : regions) {
            keys.add("payments." + region + ".created");
            keys.add("payments." + region + ".failed");
        }
        keys.add("orders.eu.created");
        keys.add("orders.eu.cancelled");
        return keys;
    }
    // endregion

    @Test
    void theRoutingKeyIsAPlainListOfWords() {
        DomainEvent event = DomainEvent.of(1, "orders.eu.created");
        assertEquals("evt_0001", event.eventId());
        assertEquals("orders.eu.created", event.routingKey());
        assertEquals("evt_0003", DomainEvent.of(3, "payments.eu.failed").eventId());
    }

    @Test
    void theEventSurvivesTheTripThroughBytes() throws IOException {
        DomainEvent event = DomainEvent.of(2, "orders.us.cancelled");
        assertEquals(event, EventJson.fromBytes(EventJson.toBytes(event)));
    }

    @Test
    void namesMatchTheEpisode() {
        assertEquals("events", Topology.EXCHANGE);
        assertEquals(4, Topology.ROUTING.size());
        assertEquals(3, Topology.COMPARISON.size());
        assertEquals("orders.*.created", Topology.ROUTING.get(0).pattern());
        assertEquals("ord*.eu.created", Topology.COMPARISON.get(2).pattern());
    }

    @Test
    void optionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"publish", "--key", "payments.eu.failed", "--first", "3"}, 1);
        assertEquals("payments.eu.failed", args.text("key", "orders.eu.created"));
        assertEquals(3, args.number("first", 1));
        assertEquals(1, args.number("count", 1));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--key"}, 1));
    }
}
