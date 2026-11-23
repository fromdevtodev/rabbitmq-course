package com.fromdevtodev.rabbitmq.fanout;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the rule and the small pieces of the project that do not talk to RabbitMQ. */
class FanoutExchangeTest {

    // region readme:test
    @Test
    void everyBoundQueueGetsACopyWhateverTheKey() {
        List<String> all = List.of("cache.instance-1", "cache.instance-2", "cache.instance-3");
        assertEquals(all, FanoutRule.queuesFor("toys", Topology.BROADCAST));
        assertEquals(all, FanoutRule.queuesFor("books", Topology.BROADCAST));
        assertEquals(all, FanoutRule.queuesFor("", Topology.BROADCAST));
    }

    @Test
    void theBindingKeysMakeNoDifference() {
        List<Binding> sameKeys = List.of(
                new Binding("cache.instance-1", "x"),
                new Binding("cache.instance-2", "x"),
                new Binding("cache.instance-3", "x"));
        assertEquals(FanoutRule.queuesFor("toys", Topology.BROADCAST), FanoutRule.queuesFor("toys", sameKeys));
    }

    @Test
    void withNoQueueBoundThereIsNobodyToCopyTo() {
        assertEquals(List.of(), FanoutRule.queuesFor("toys", List.of()));
    }
    // endregion

    @Test
    void aNewQueueIsAFourthCopyAndNothingElseChanges() {
        List<Binding> four = List.of(
                new Binding("cache.instance-1", ""),
                new Binding("cache.instance-2", "books"),
                new Binding("cache.instance-3", "electronics"),
                new Binding("cache.instance-4", "anything"));
        assertEquals(4, FanoutRule.queuesFor("toys", four).size());
    }

    @Test
    void priceUpdatesAreNumberedAndRoundTripThroughJson() throws IOException {
        PriceUpdate update = PriceUpdate.of(7, "toys");
        assertEquals("evt_0007", update.eventId());
        assertEquals("toys", update.routingKey());
        assertEquals(update, EventJson.fromBytes(EventJson.toBytes(update)));
    }

    @Test
    void namesMatchTheEpisode() {
        assertEquals("price-updates", Topology.EXCHANGE);
        assertEquals(3, Topology.BROADCAST.size());
        assertEquals("", Topology.BROADCAST.get(0).key());
        assertEquals("books", Topology.BROADCAST.get(1).key());
        assertEquals("electronics", Topology.BROADCAST.get(2).key());
        assertEquals("cache.shared", Topology.SHARED);
    }

    @Test
    void optionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"publish", "--key", "toys", "--first", "3"}, 1);
        assertEquals("toys", args.text("key", "books"));
        assertEquals(3, args.number("first", 1));
        assertEquals(1, args.number("count", 1));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--key"}, 1));
    }

    @Test
    void aConsumerNeedsAName() {
        Args args = new Args(new String[] {"consume"}, 1);
        assertThrows(IllegalArgumentException.class, () -> args.required("name"));
    }
}
