package com.fromdevtodev.rabbitmq.nack;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the rules and the small pieces of the project that do not talk to RabbitMQ. */
class NackAndRequeueTest {

    // region readme:test
    @Test
    void aValidOrderIsAcknowledged() {
        OrderEvent order = OrderEvent.of(1, false);
        assertEquals(Rules.Failure.NONE, Rules.check(order));
        assertEquals(Rules.Outcome.ACK, Rules.decide(Rules.check(order), Rules.OnInvalid.REQUEUE));
        assertEquals(Rules.Outcome.ACK, Rules.decide(Rules.check(order), Rules.OnInvalid.DISCARD));
    }

    @Test
    void anOrderWithoutAnEmailAddressIsAPermanentFailure() {
        OrderEvent order = OrderEvent.of(2, true);
        assertFalse(order.hasEmailAddress());
        assertEquals(Rules.Failure.INVALID_ORDER, Rules.check(order));
    }

    @Test
    void theLoopIsNackWithRequeueAndTheFixIsNackWithoutIt() {
        assertEquals(Rules.Outcome.NACK_REQUEUE, Rules.decide(Rules.Failure.INVALID_ORDER, Rules.OnInvalid.REQUEUE));
        assertEquals(Rules.Outcome.NACK_DISCARD, Rules.decide(Rules.Failure.INVALID_ORDER, Rules.OnInvalid.DISCARD));
        assertTrue(Rules.requeue(Rules.Outcome.NACK_REQUEUE));
        assertFalse(Rules.requeue(Rules.Outcome.NACK_DISCARD));
        assertFalse(Rules.requeue(Rules.Outcome.ACK));
    }

    @Test
    void aTransientFailureIsRequeuedWhateverTheSetting() {
        assertEquals(Rules.Outcome.NACK_REQUEUE, Rules.decide(Rules.Failure.MAIL_SERVER_DOWN, Rules.OnInvalid.REQUEUE));
        assertEquals(Rules.Outcome.NACK_REQUEUE, Rules.decide(Rules.Failure.MAIL_SERVER_DOWN, Rules.OnInvalid.DISCARD));
    }
    // endregion

    @Test
    void theWorkerCountsItsOwnAttempts() {
        Attempts attempts = new Attempts();
        assertEquals(0, attempts.of("evt_0002"));
        assertEquals(1, attempts.next("evt_0002"));
        assertEquals(2, attempts.next("evt_0002"));
        assertEquals(1, attempts.next("evt_0003"));
        assertEquals(2, attempts.of("evt_0002"));
    }

    @Test
    void theRunningExampleUsesTheSheetsIds() {
        assertEquals("evt_0001", OrderEvent.of(1, false).eventId());
        assertEquals("ord_1041", OrderEvent.of(1, false).orderId());
        assertEquals("evt_0002", OrderEvent.of(2, true).eventId());
        assertEquals("ord_1042", OrderEvent.of(2, true).orderId());
        assertEquals("ord_1043", OrderEvent.of(3, false).orderId());
        assertNull(OrderEvent.of(2, true).customerEmail());
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
    }

    @Test
    void optionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"work", "--on-invalid", "discard", "--work-ms", "250"}, 1);
        assertEquals("discard", args.text("on-invalid", "requeue"));
        assertEquals(250, args.number("work-ms", 700));
        assertEquals(0, args.number("max-deliveries", 0));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"work", "--on-invalid"}, 1));
    }
}
