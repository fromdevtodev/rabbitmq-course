package com.fromdevtodev.rabbitmq.exchanges;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for the parts that need no running broker. Routing itself is the broker's job; see the experiments in the README. */
class ExchangesTest {

    @Test
    void messagesAreNumberedAndCarryTheirType() {
        OrderEvent created = OrderEvent.of(1, "order.created");
        assertEquals("evt_0001", created.eventId());
        assertEquals("ord_1001", created.orderId());
        assertEquals("order.created", created.type());

        OrderEvent cancelled = OrderEvent.of(12, "order.cancelled");
        assertEquals("evt_0012", cancelled.eventId());
        assertEquals("ord_1012", cancelled.orderId());
        assertEquals("order.cancelled", cancelled.type());
    }

    @Test
    void messageBodySurvivesAJsonRoundTrip() throws Exception {
        OrderEvent original = OrderEvent.of(3, "order.cancelled");
        assertEquals(original, EventJson.fromBytes(EventJson.toBytes(original)));
    }

    @Test
    void statusLineShowsEveryQueueAndMarksMissingOnes() {
        Map<String, Long> ready = new LinkedHashMap<>();
        ready.put("notifications.email", 2L);
        ready.put("loyalty.points", null);
        ready.put("audit.log", 0L);
        assertEquals(
                "14:03:07 | notifications.email ready 2 | loyalty.points does not exist yet | audit.log ready 0",
                StatusLine.format(LocalTime.of(14, 3, 7, 999_000_000), ready));
    }

    @Test
    void statusLineKeepsTheSecondsEvenWhenTheyAreZero() {
        Map<String, Long> ready = new LinkedHashMap<>();
        ready.put("audit.log", 1L);
        assertEquals("09:30:00 | audit.log ready 1", StatusLine.format(LocalTime.of(9, 30, 0), ready));
    }

    @Test
    void theStoryStartsWithoutABindingForLoyaltyPoints() {
        List<Topology.Binding> start = Topology.INITIAL_BINDINGS;
        assertTrue(start.contains(new Topology.Binding("notifications.email", "order.created")));
        assertTrue(start.contains(new Topology.Binding("audit.log", "order.cancelled")));
        assertFalse(start.stream().anyMatch(b -> b.queue().equals(Topology.POINTS)),
                "loyalty.points is added later, by the points worker");
        assertTrue(start.stream().allMatch(b -> Topology.QUEUES.contains(b.queue())));
    }

    @Test
    void argsParseNumbersTextAndRequiredOptions() {
        Args args = new Args(new String[] {"bind", "--queue", "loyalty.points", "--key", "order.created", "--count", "5"}, 1);
        assertEquals("loyalty.points", args.required("queue"));
        assertEquals("order.created", args.text("key", "other"));
        assertEquals("order.created", args.text("missing", "order.created"));
        assertEquals(5L, args.number("count", 1));
        assertEquals(1L, args.number("first", 1));
        assertThrows(IllegalArgumentException.class, () -> args.required("missing"));
    }

    @Test
    void argsRejectMalformedInput() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "key", "order.created"}, 1));
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--key"}, 1));
    }
}
