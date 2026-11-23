package com.fromdevtodev.rabbitmq.queues;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the pieces of the project that do not talk to RabbitMQ. */
class PrefetchTest {

    // region readme:test
    @Test
    void messagesAreNumberedInPublishingOrder() {
        assertEquals("evt_0001", OrderEvent.of(1).eventId());
        assertEquals("ord_1001", OrderEvent.of(1).orderId());
        assertEquals("evt_0005", OrderEvent.of(5).eventId());
        assertEquals(OrderEvent.TYPE, OrderEvent.of(3).type());
    }
    // endregion

    @Test
    void theEventSurvivesTheTripThroughBytes() throws IOException {
        OrderEvent event = OrderEvent.of(4);
        assertEquals(event, EventJson.fromBytes(EventJson.toBytes(event)));
    }

    @Test
    void optionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"work", "--name", "slow", "--prefetch", "1", "--limit", "5"}, 1);
        assertEquals("slow", args.text("name", "worker"));
        assertEquals(1, args.number("prefetch", 0));
        assertEquals(5, args.number("limit", 0));
        assertEquals(500, args.number("delay-ms", 500));
        assertEquals(0, new Args(new String[] {"work"}, 1).number("prefetch", 0), "no --prefetch means 0: no limit");
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--count"}, 1));
    }

    @Test
    void aMissingRequiredOptionIsReported() {
        Args args = new Args(new String[] {"work"}, 1);
        assertThrows(IllegalArgumentException.class, () -> args.required("name"));
    }
}
