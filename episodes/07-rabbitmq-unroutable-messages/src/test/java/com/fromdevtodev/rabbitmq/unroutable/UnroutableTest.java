package com.fromdevtodev.rabbitmq.unroutable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Executable versions of the claims in the Unroutable Messages episode. Routing and
 * unroutability are predicted by {@link Rules}; no broker is required.
 */
class UnroutableTest {

    @Test
    void createdMessagesReachEmailPointsAndAudit() {
        assertEquals(
                List.of(Rules.EMAIL, Rules.POINTS, Rules.AUDIT),
                Rules.queuesFor(Rules.CREATED));
    }

    @Test
    void cancelledMessagesReachEmailAndAuditButNotPoints() {
        assertEquals(
                List.of(Rules.EMAIL, Rules.AUDIT),
                Rules.queuesFor(Rules.CANCELLED));
    }

    @Test
    void theTypoIsUnroutable() {
        assertTrue(Rules.isUnroutable(Rules.CANCELED),
                "order.canceled (one L) matches no binding");
        assertTrue(Rules.queuesFor(Rules.CANCELED).isEmpty());
    }

    @Test
    void unknownKeyIsUnroutable() {
        assertTrue(Rules.isUnroutable("order.shipped"));
        assertFalse(Rules.isUnroutable(Rules.CREATED));
    }

    @Test
    void alternateExchangeAndParkingQueueAreDocumented() {
        assertEquals("orders.unrouted", Rules.ALTERNATE_EXCHANGE);
        assertEquals("unrouted.parking", Rules.PARKING_QUEUE);
    }

    @Test
    void messageBodySurvivesAJsonRoundTrip() throws Exception {
        OrderEvent original = OrderEvent.of(3, Rules.CANCELLED);
        assertEquals(original, EventJson.fromBytes(EventJson.toBytes(original)));
    }

    @Test
    void statusLineShowsEveryQueueAndMarksMissingOnes() {
        Map<String, Long> ready = new LinkedHashMap<>();
        ready.put(Rules.EMAIL, 2L);
        ready.put(Rules.POINTS, null);
        ready.put(Rules.AUDIT, 0L);
        assertEquals(
                "14:03:07 | notifications.email ready 2 | loyalty.points does not exist yet | audit.log ready 0",
                StatusLine.format(LocalTime.of(14, 3, 7, 999_000_000), ready));
    }

    @Test
    void argsParseNumbersTextAndRequiredOptions() {
        Args args = new Args(new String[] {"publish", "--key", "order.created", "--count", "5"}, 1);
        assertEquals("order.created", args.required("key"));
        assertEquals(5L, args.number("count", 1));
        assertThrows(IllegalArgumentException.class, () -> args.required("missing"));
    }

    @Test
    void argsRejectMalformedInput() {
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"publish", "key", "order.created"}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"publish", "--key"}, 1));
    }
}
