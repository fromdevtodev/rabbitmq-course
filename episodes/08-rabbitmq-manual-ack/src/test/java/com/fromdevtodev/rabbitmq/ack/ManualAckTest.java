package com.fromdevtodev.rabbitmq.ack;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class ManualAckTest {

    @Test
    void automaticAndManualModesMapToBasicConsumeContract() {
        assertTrue(AckMode.AUTOMATIC.autoAck());
        assertFalse(AckMode.MANUAL.autoAck());
        assertEquals(AckMode.AUTOMATIC, AckMode.parse("auto"));
        assertEquals(AckMode.AUTOMATIC, AckMode.parse(" AUTOMATIC "));
        assertEquals(AckMode.MANUAL, AckMode.parse(" Manual "));
        assertThrows(IllegalArgumentException.class, () -> AckMode.parse("sometimes"));
        assertThrows(NullPointerException.class, () -> AckMode.parse(null));
    }

    @Test
    void crashPointsCoverBeforeWorkDuringWorkAndAfterWorkBeforeAck() {
        assertEquals(CrashPoint.NONE, CrashPoint.parse("none"));
        assertEquals(CrashPoint.WHILE_SENDING, CrashPoint.parse("while-sending"));
        assertEquals(CrashPoint.AFTER_SEND, CrashPoint.parse("after-send"));
        assertThrows(IllegalArgumentException.class, () -> CrashPoint.parse("whenever"));
        assertThrows(NullPointerException.class, () -> CrashPoint.parse(null));
    }

    @Test
    void queueAndEventContractAreStable() {
        assertEquals("notifications.email", Topology.QUEUE);
        assertEquals("order.created", OrderEvent.TYPE);
        assertEquals("evt_0001", OrderEvent.of(1).eventId());
        assertEquals("ord_1001", OrderEvent.of(1).orderId());
        assertEquals("evt_0012", OrderEvent.of(12).eventId());
        assertEquals("ord_1012", OrderEvent.of(12).orderId());
    }

    @Test
    void eventSurvivesJsonRoundTrip() throws IOException {
        OrderEvent event = OrderEvent.of(3);
        assertEquals(event, EventJson.fromBytes(EventJson.toBytes(event)));
    }

    @Test
    void workerArgumentsParseValuesAndDefaults() {
        Args args = new Args(new String[] {
                "work", "--mode", "manual", "--crash", "after-send", "--send-ms", "2500", "--limit", "3"
        }, 1);
        assertEquals("manual", args.required("mode"));
        assertEquals("after-send", args.text("crash", "none"));
        assertEquals(2500L, args.number("send-ms", 4000));
        assertEquals(3L, args.number("limit", 0));
        assertEquals("email-worker", args.text("name", "email-worker"));
    }

    @Test
    void malformedAndMissingArgumentsAreRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"work", "mode", "manual"}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"work", "--mode"}, 1));
        Args args = new Args(new String[] {"work"}, 1);
        assertThrows(IllegalArgumentException.class, () -> args.required("mode"));
        assertThrows(IllegalArgumentException.class, () -> args.required("blank"));
    }

    @Test
    void ackModeHasExactlyOneAutomaticAndOneManualChoice() {
        assertEquals(2, AckMode.values().length);
        assertEquals(1, java.util.Arrays.stream(AckMode.values()).filter(AckMode::autoAck).count());
        assertEquals(1, java.util.Arrays.stream(AckMode.values()).filter(mode -> !mode.autoAck()).count());
    }
}
