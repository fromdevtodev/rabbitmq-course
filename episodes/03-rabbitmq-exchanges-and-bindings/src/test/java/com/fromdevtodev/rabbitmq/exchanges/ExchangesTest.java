package com.fromdevtodev.rabbitmq.exchanges;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the pieces of the project that do not talk to RabbitMQ. */
class ExchangesTest {

    // region readme:test
    @Test
    void theEventTypeIsTheRoutingKeyItIsPublishedWith() {
        assertEquals("evt_0001", OrderEvent.of(1, OrderEvent.CREATED).eventId());
        assertEquals("ord_1001", OrderEvent.of(1, OrderEvent.CREATED).orderId());
        assertEquals(OrderEvent.CANCELLED, OrderEvent.of(3, OrderEvent.CANCELLED).type());
        assertEquals("evt_0003", OrderEvent.of(3, OrderEvent.CANCELLED).eventId());
    }
    // endregion

    @Test
    void theEventSurvivesTheTripThroughBytes() throws IOException {
        OrderEvent event = OrderEvent.of(2, OrderEvent.CREATED);
        assertEquals(event, EventJson.fromBytes(EventJson.toBytes(event)));
    }

    @Test
    void namesMatchTheEpisode() {
        assertEquals("orders", Topology.EXCHANGE);
        assertEquals("notifications.email", Topology.EMAIL);
        assertEquals("loyalty.points", Topology.POINTS);
        assertEquals("audit.log", Topology.AUDIT);
    }

    @Test
    void optionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"publish", "--key", "order.cancelled", "--first", "3"}, 1);
        assertEquals("order.cancelled", args.text("key", OrderEvent.CREATED));
        assertEquals(3, args.number("first", 1));
        assertEquals(1, args.number("count", 1));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--key"}, 1));
    }
}
