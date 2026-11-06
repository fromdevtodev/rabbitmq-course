package com.fromdevtodev.rabbitmq.queues;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueuesTest {

    /** A fake clock: sleeping just moves time forward, so the test takes no real time. */
    private static final class FakeTime {
        long now;
        final List<Long> sleeps = new ArrayList<>();

        RatePacer pacer(double perSecond) {
            return new RatePacer(perSecond, () -> now, nanos -> {
                sleeps.add(nanos);
                now += nanos;
            });
        }
    }

    @Test
    void sixtyPerSecondTakesOneSecondForSixtyOneEvents() throws Exception {
        FakeTime time = new FakeTime();
        RatePacer pacer = time.pacer(60);
        for (int i = 0; i < 61; i++) {
            pacer.pace();
        }
        // The first event is free; the 60 after it span exactly one second.
        assertTrue(Math.abs(time.now - 1_000_000_000L) < 1_000, "took " + time.now + " ns");
    }

    @Test
    void lateEventsAreCaughtUpInsteadOfAccumulatingDelay() throws Exception {
        FakeTime time = new FakeTime();
        RatePacer pacer = time.pacer(10); // 100 ms apart
        pacer.pace();
        time.now += 350_000_000L; // the work took far too long
        int before = time.sleeps.size();
        pacer.pace();
        pacer.pace();
        pacer.pace();
        assertEquals(before, time.sleeps.size(), "late events must not sleep");
        pacer.pace(); // back on schedule: now it has to wait
        assertTrue(time.sleeps.size() > before);
    }

    @Test
    void zeroRateMeansNoLimit() throws Exception {
        FakeTime time = new FakeTime();
        RatePacer pacer = time.pacer(0);
        for (int i = 0; i < 1000; i++) {
            pacer.pace();
        }
        assertEquals(0L, time.now);
        assertTrue(time.sleeps.isEmpty());
    }

    @Test
    void messagesAreNumberedInOrder() {
        OrderCreated first = OrderCreated.of(1);
        assertEquals("evt_0001", first.eventId());
        assertEquals("ord_1001", first.orderId());
        assertEquals("order.created", first.type());
        assertEquals("evt_0012", OrderCreated.of(12).eventId());
        assertEquals("ord_1012", OrderCreated.of(12).orderId());
    }

    @Test
    void statusLineShowsReadyAndConsumers() {
        LocalTime t = LocalTime.of(14, 3, 7, 999_000_000);
        assertEquals("14:03:07 | notifications.email | ready 12000 | consumers 0",
                StatusLine.format(t, "notifications.email", 12_000, 0));
        assertEquals("14:03:07 | notifications.email | queue does not exist yet",
                StatusLine.missing(t, "notifications.email"));
    }

    @Test
    void argsParseNumbersAndRejectMalformedInput() {
        Args args = new Args(new String[] {"publish", "--count", "50", "--rate", "100"}, 1);
        assertEquals(50L, args.number("count", 4));
        assertEquals(100L, args.number("rate", 0));
        assertEquals(1L, args.number("first", 1));
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "count", "5"}, 1));
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--count"}, 1));
    }

    @Test
    void argsParseDecimalRates() {
        Args args = new Args(new String[] {"consume", "--rate", "0.5"}, 1);
        assertEquals(0.5, args.decimal("rate", 1), 0.0);
        assertEquals(1.0, args.decimal("missing", 1), 0.0);
        assertEquals(100.0, new Args(new String[] {"consume", "--rate", "100"}, 1).decimal("rate", 1), 0.0);
    }
}
