package com.fromdevtodev.rabbitmq.manualack;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the pieces of the project that do not talk to RabbitMQ. */
class ManualAckTest {

    // region readme:test
    @Test
    void automaticModeAsksTheBrokerToForgetAtDelivery() {
        assertTrue(AckMode.parse("auto").autoAck());
        assertFalse(AckMode.parse("manual").autoAck());
        assertSame(AckMode.AUTO, AckMode.parse("automatic"));
    }
    // endregion

    @Test
    void anUnknownModeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> AckMode.parse("sometimes"));
    }

    @Test
    void crashPointsAreParsed() {
        assertSame(CrashPoint.NONE, CrashPoint.parse("none"));
        assertSame(CrashPoint.WHILE_SENDING, CrashPoint.parse("while-sending"));
        assertSame(CrashPoint.AFTER_SEND, CrashPoint.parse("AFTER-SEND"));
        assertThrows(IllegalArgumentException.class, () -> CrashPoint.parse("never"));
    }

    @Test
    void messagesAreNumberedInPublishingOrder() {
        assertEquals("evt_0001", OrderEvent.of(1).eventId());
        assertEquals("ord_1001", OrderEvent.of(1).orderId());
        assertEquals(OrderEvent.TYPE, OrderEvent.of(3).type());
    }

    @Test
    void theEventSurvivesTheTripThroughBytes() throws IOException {
        OrderEvent event = OrderEvent.of(4);
        assertEquals(event, EventJson.fromBytes(EventJson.toBytes(event)));
    }

    @Test
    void workerOptionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"work", "--name", "email-worker-1", "--mode", "auto", "--crash", "while-sending"}, 1);
        assertEquals("email-worker-1", args.text("name", "email-worker"));
        assertEquals(AckMode.AUTO, AckMode.parse(args.text("mode", "manual")));
        assertEquals(CrashPoint.WHILE_SENDING, CrashPoint.parse(args.text("crash", "none")));
        assertEquals(3_000, args.number("send-ms", 3_000));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--count"}, 1));
    }
}
