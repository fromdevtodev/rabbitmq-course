package com.fromdevtodev.rabbitmq.dlx;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the rules and the small pieces of the project that do not talk to RabbitMQ. */
class DeadLetterTest {

    // region readme:test
    @Test
    void aValidOrderIsAcknowledged() {
        assertEquals(Rules.Outcome.ACK, Rules.decide(Rules.check(OrderEvent.of(1, false))));
    }

    @Test
    void anOrderWithoutAnEmailAddressIsNackedWithoutRequeue() {
        OrderEvent order = OrderEvent.of(2, true);
        assertEquals(Rules.Failure.INVALID_ORDER, Rules.check(order));
        Rules.Outcome outcome = Rules.decide(Rules.check(order));
        assertEquals(Rules.Outcome.NACK_DISCARD, outcome);
        assertFalse(Rules.requeue(outcome));
    }

    @Test
    void theXDeathHeaderIsReadEntryByEntry() {
        Object header = List.of(Map.of(
                "queue", "notifications.email", "reason", "rejected", "count", 1L,
                "exchange", "orders", "routing-keys", List.of("order.created")));
        List<XDeath.Death> deaths = XDeath.parse(header);
        assertEquals(1, deaths.size());
        assertEquals("rejected", deaths.get(0).reason());
        assertEquals(
                "reason=rejected, queue=notifications.email, exchange=orders, count=1, routing-keys=[order.created]",
                deaths.get(0).describe());
    }
    // endregion

    @Test
    void aMessageThatWasNeverDeadLetteredHasNoXDeath() {
        assertTrue(XDeath.parse(null).isEmpty());
        assertTrue(XDeath.parse("not a list").isEmpty());
    }

    @Test
    void theRunningExampleUsesTheSheetsIds() {
        assertEquals("evt_0002", OrderEvent.of(2, true).eventId());
        assertEquals("ord_1042", OrderEvent.of(2, true).orderId());
        assertNull(OrderEvent.of(2, true).customerEmail());
        assertEquals("ord_1043", OrderEvent.of(3, false).orderId());
    }

    @Test
    void theEventSurvivesTheTripThroughBytes() throws IOException {
        OrderEvent order = OrderEvent.of(2, true);
        assertEquals(order, EventJson.fromBytes(EventJson.toBytes(order)));
    }

    @Test
    void namesMatchTheEpisode() {
        assertEquals("orders", Topology.EXCHANGE);
        assertEquals("notifications.email", Topology.QUEUE);
        assertEquals("order.created", Topology.ROUTING_KEY);
        assertEquals("orders.dlx", Topology.DEAD_LETTER_EXCHANGE);
        assertEquals("notifications.email.dlq", Topology.DEAD_LETTER_QUEUE);
    }

    @Test
    void optionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"setup", "--stage", "source", "--dlq-key", "order.failed"}, 1);
        assertEquals("source", args.text("stage", "all"));
        assertEquals("order.failed", args.text("dlq-key", "order.created"));
        assertEquals(0, args.number("max-length", 0));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"work", "--work-ms"}, 1));
    }
}
