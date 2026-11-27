package com.fromdevtodev.rabbitmq.retry;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** No broker needed: the rules, the fake mail server, the x-death reader and the small pieces. */
class DelayedRetryTest {

    // region readme:test
    @Test
    void theThreeSettingsAnswerTheSameFailureDifferently() {
        Rules.Failure down = Rules.Failure.MAIL_SERVER_DOWN;
        assertEquals(Rules.Outcome.NACK_REQUEUE, Rules.decide(down, Rules.OnFailure.REQUEUE));
        assertEquals(Rules.Outcome.SLEEP_THEN_REQUEUE, Rules.decide(down, Rules.OnFailure.SLEEP));
        assertEquals(Rules.Outcome.NACK_DEAD_LETTER, Rules.decide(down, Rules.OnFailure.RETRY_QUEUE));
    }

    @Test
    void onlyTheRetryQueueSettingNacksWithoutRequeue() {
        assertTrue(Rules.requeue(Rules.Outcome.NACK_REQUEUE));
        assertTrue(Rules.requeue(Rules.Outcome.SLEEP_THEN_REQUEUE));
        assertFalse(Rules.requeue(Rules.Outcome.NACK_DEAD_LETTER));
        assertFalse(Rules.requeue(Rules.Outcome.ACK));
    }

    @Test
    void successIsAcknowledgedWhateverTheSetting() {
        for (Rules.OnFailure setting : Rules.OnFailure.values()) {
            assertEquals(Rules.Outcome.ACK, Rules.decide(Rules.Failure.NONE, setting));
        }
    }
    // endregion

    @Test
    void theMailServerIsDownForTheWindowAndThenRecovers() {
        AtomicLong now = new AtomicLong(1_000);
        MailServer mail = new MailServer(Set.of("evt_0002"), 25_000, now::get);
        OrderEvent bad = OrderEvent.of(2);
        int failures = 0;
        for (int call = 0; call < 4; call++) {              // a 10-second delay: calls at about 0, 10, 20 and 30 seconds
            if (mail.send(bad) == Rules.Failure.MAIL_SERVER_DOWN) {
                failures++;
            }
            now.addAndGet(10_000);
        }
        assertEquals(3, failures);                           // the first three fall inside the 25-second outage
        assertEquals(4, mail.callsFor("evt_0002"));
    }

    @Test
    void xDeathCountsOnlyRejectionsByTheWorkQueue() {
        Map<String, Object> rejected = Map.of("queue", "notifications.email", "reason", "rejected", "count", 2L);
        Map<String, Object> expired = Map.of("queue", "notifications.email.retry", "reason", "expired", "count", 2L);
        Map<String, Object> headers = Map.of("x-death", List.of(expired, rejected));
        assertEquals(2, Deaths.rejections(headers, "notifications.email"));
        assertEquals(0, Deaths.rejections(Map.of(), "notifications.email"));
        assertEquals(0, Deaths.rejections(null, "notifications.email"));
    }

    @Test
    void theWorkerCountsItsOwnAttempts() {
        Attempts attempts = new Attempts();
        assertEquals(1, attempts.next("evt_0002"));
        assertEquals(2, attempts.next("evt_0002"));
        assertEquals(1, attempts.next("evt_0003"));
    }

    @Test
    void theRunningExampleUsesTheSheetsIds() {
        assertEquals("evt_0002", OrderEvent.of(2).eventId());
        assertEquals("ord_1042", OrderEvent.of(2).orderId());
        assertEquals("customer-1043@example.com", OrderEvent.of(3).customerEmail());
    }

    @Test
    void theEventRoundTripsThroughJson() throws IOException {
        OrderEvent event = OrderEvent.of(2);
        assertEquals(event, EventJson.fromBytes(EventJson.toBytes(event)));
    }

    @Test
    void argsParseNamedOptions() {
        Args args = new Args(new String[] {"work", "--work-ms", "300", "--on-failure", "retry-queue"}, 1);
        assertEquals(300, args.number("work-ms", 0));
        assertEquals("retry-queue", args.text("on-failure", "requeue"));
        assertEquals(8, args.number("down-seconds", 8));
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"work", "oops"}, 1));
    }
}
