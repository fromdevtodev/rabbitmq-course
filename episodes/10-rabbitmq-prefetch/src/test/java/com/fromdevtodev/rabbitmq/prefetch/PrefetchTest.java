package com.fromdevtodev.rabbitmq.prefetch;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PrefetchTest {

    @Test
    void queueNameIsStable() {
        assertEquals("notifications.email", Topology.QUEUE);
    }

    @Test
    void tallyTracksReceivedAcknowledgedAndInFlight() {
        Tally tally = new Tally();
        assertEquals(0, tally.inFlight());
        assertEquals(0, tally.handled());
        assertEquals(0, tally.receivedCount());

        tally.received();
        tally.received();
        tally.received();
        assertEquals(3, tally.receivedCount());
        assertEquals(3, tally.inFlight());
        assertEquals(0, tally.handled());

        tally.acknowledged();
        tally.acknowledged();
        assertEquals(1, tally.inFlight());
        assertEquals(2, tally.handled());
        tally.received();
        tally.acknowledged();
        assertEquals(0, tally.inFlight());
        assertEquals(3, tally.handled());
    }

    @Test
    void unlimitedOrNonPositiveRateNeverSleeps() throws Exception {
        for (double rate : new double[] {0, -1}) {
            List<Long> sleeps = new ArrayList<>();
            RatePacer pacer = new RatePacer(rate, () -> 0L, sleeps::add);
            pacer.pace();
            pacer.pace();
            assertTrue(sleeps.isEmpty());
        }
    }

    @Test
    void firstEventIsImmediateAndFollowingEventsRespectRate() throws Exception {
        AtomicLong clock = new AtomicLong(1_000_000_000L);
        List<Long> sleeps = new ArrayList<>();
        RatePacer pacer = new RatePacer(2.0, clock::get, nanos -> {
            sleeps.add(nanos);
            clock.addAndGet(nanos);
        });

        pacer.pace();
        assertTrue(sleeps.isEmpty());
        pacer.pace();
        assertEquals(List.of(500_000_000L), sleeps);
        pacer.pace();
        assertEquals(List.of(500_000_000L, 500_000_000L), sleeps);
    }

    @Test
    void pacerCatchesUpInsteadOfAccumulatingLateDelays() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        List<Long> sleeps = new ArrayList<>();
        RatePacer pacer = new RatePacer(2.0, clock::get, nanos -> {
            sleeps.add(nanos);
            clock.addAndGet(nanos);
        });

        pacer.pace();             // due at 0; next due at 500 ms
        clock.set(1_200_000_000L); // work took longer than two intervals
        pacer.pace();             // late event does not sleep
        pacer.pace();             // next scheduled event is also already due
        assertTrue(sleeps.isEmpty());
    }

    @Test
    void statusLineFormatsQueueDepthAndMissingQueue() {
        assertEquals("14:03:07 | notifications.email | ready 12 | consumers 2",
                StatusLine.format(LocalTime.of(14, 3, 7), Topology.QUEUE, 12, 2));
        assertEquals("09:30:00 | notifications.email | queue does not exist yet",
                StatusLine.missing(LocalTime.of(9, 30, 0), Topology.QUEUE));
    }

    @Test
    void reportLineFormatsConsumerInFlightAndHandledCounts() {
        assertEquals("14:03:07 [slow] in flight 5 | handled 12",
                ReportLine.format(LocalTime.of(14, 3, 7), "slow", 5, 12));
    }

    @Test
    void eventSurvivesJsonRoundTripWithExpectedIdentifiers() throws Exception {
        OrderCreated original = OrderCreated.of(7);
        assertEquals(original, EventJson.fromBytes(EventJson.toBytes(original)));
        assertEquals("evt_0007", original.eventId());
        assertEquals("ord_1007", original.orderId());
    }

    @Test
    void argsParseOptionsAndDefaults() {
        Args args = new Args(new String[] {
                "consume", "--name", "fast", "--rate", "20", "--prefetch", "5", "--report-ms", "500"
        }, 1);
        assertEquals("fast", args.text("name", "worker"));
        assertEquals(20L, args.number("rate", 1));
        assertEquals(5L, args.number("prefetch", 1));
        assertEquals(500L, args.number("report-ms", 1000));

        Args defaults = new Args(new String[] {"consume"}, 1);
        assertEquals("worker", defaults.text("name", "worker"));
        assertEquals(1L, defaults.number("prefetch", 1));
    }

    @Test
    void argsRejectMalformedPairs() {
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"consume", "prefetch", "2"}, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new Args(new String[] {"consume", "--prefetch"}, 1));
    }
}
