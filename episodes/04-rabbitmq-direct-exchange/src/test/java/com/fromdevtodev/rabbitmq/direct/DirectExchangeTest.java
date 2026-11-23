package com.fromdevtodev.rabbitmq.direct;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the rule and the small pieces of the project that do not talk to RabbitMQ. */
class DirectExchangeTest {

    // region readme:test
    @Test
    void everyQueueWithAnEqualBindingKeyGetsACopy() {
        assertEquals(Set.of("errors-pager", "audit"), DirectRule.queuesFor("error", Topology.BINDINGS));
        assertEquals(Set.of("audit", "metrics"), DirectRule.queuesFor("warning", Topology.BINDINGS));
    }

    @Test
    void aQueueBoundSeveralTimesStillGetsOneCopyPerMessage() {
        assertEquals(1, DirectRule.queuesFor("info", Topology.BINDINGS).size());
        assertEquals(Set.of("audit"), DirectRule.queuesFor("info", Topology.BINDINGS));
    }

    @Test
    void aKeyThatMatchesNoBindingDeliversNothing() {
        assertTrue(DirectRule.queuesFor("debug", Topology.BINDINGS).isEmpty());
    }

    @Test
    void matchingIsExactNotPartial() {
        List<Binding> bindings = List.of(new Binding("errors-pager", "error"));
        assertTrue(DirectRule.queuesFor("errors", bindings).isEmpty());
        assertTrue(DirectRule.queuesFor("Error", bindings).isEmpty());
        assertTrue(DirectRule.queuesFor("err", bindings).isEmpty());
    }
    // endregion

    @Test
    void theLevelIsTheRoutingKeyTheLineIsPublishedWith() {
        assertEquals("log_0001", LogEvent.of(1, LogEvent.ERROR).eventId());
        assertEquals("error", LogEvent.of(1, LogEvent.ERROR).level());
        assertEquals("log_0003", LogEvent.of(3, LogEvent.DEBUG).eventId());
    }

    @Test
    void theEventSurvivesTheTripThroughBytes() throws IOException {
        LogEvent event = LogEvent.of(2, LogEvent.WARNING);
        assertEquals(event, EventJson.fromBytes(EventJson.toBytes(event)));
    }

    @Test
    void namesMatchTheEpisode() {
        assertEquals("logs", Topology.EXCHANGE);
        assertEquals("errors-pager", Topology.PAGER);
        assertEquals("audit", Topology.AUDIT);
        assertEquals("metrics", Topology.METRICS);
        assertEquals("notifications.email", Topology.EMAIL);
        assertEquals(5, Topology.BINDINGS.size());
    }

    @Test
    void optionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"publish", "--key", "debug", "--first", "3"}, 1);
        assertEquals("debug", args.text("key", LogEvent.ERROR));
        assertEquals(3, args.number("first", 1));
        assertEquals(1, args.number("count", 1));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--key"}, 1));
    }
}
