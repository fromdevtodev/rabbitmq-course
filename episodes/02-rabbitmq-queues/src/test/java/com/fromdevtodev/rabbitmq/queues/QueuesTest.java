package com.fromdevtodev.rabbitmq.queues;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the pieces of the project that do not talk to RabbitMQ. */
class QueuesTest {

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
        Args args = new Args(new String[] {"consume", "--name", "consumer-a", "--limit", "5"}, 1);
        assertEquals("consumer-a", args.text("name", "consumer"));
        assertEquals(5, args.number("limit", 0));
        assertEquals(1_500, args.number("delay-ms", 1_500));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--count"}, 1));
    }

    @Test
    void aMissingRequiredOptionIsReported() {
        Args args = new Args(new String[] {"consume"}, 1);
        assertThrows(IllegalArgumentException.class, () -> args.required("name"));
    }
}
