package com.fromdevtodev.rabbitmq.nack;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Executable versions of the claims in the Nack and Requeue episode.
 * {@link Rules#decide} is the pure model of what the worker answers; the broker behaviour
 * is shown in the README experiments.
 */
class NackAndRequeueTest {

    @Test
    void successfulWorkIsAlwaysAcked() {
        Rules.Decision d = Rules.decide(Rules.OnFailure.REQUEUE, null);
        assertTrue(d.ack());
        assertEquals("ack", d.describe());
    }

    @Test
    void requeuePolicyPutsEveryFailureBack() {
        Rules.Decision d = Rules.decide(
                Rules.OnFailure.REQUEUE,
                new EmailWorker.InvalidOrderException("no email"));
        assertFalse(d.ack());
        assertTrue(d.requeue());
        assertEquals("nack, requeue=true", d.describe());
    }

    @Test
    void discardPolicyRemovesEveryFailure() {
        Rules.Decision invalid = Rules.decide(
                Rules.OnFailure.DISCARD,
                new EmailWorker.InvalidOrderException("no email"));
        Rules.Decision transientFail = Rules.decide(
                Rules.OnFailure.DISCARD,
                new EmailWorker.TransientFailure("mail server down"));
        assertFalse(invalid.ack());
        assertFalse(invalid.requeue());
        assertFalse(transientFail.requeue());
    }

    @Test
    void byKindNeverRequeuesAnInvalidOrder() {
        Rules.Decision invalid = Rules.decide(
                Rules.OnFailure.BY_KIND,
                new EmailWorker.InvalidOrderException("no email"));
        assertFalse(invalid.ack());
        assertFalse(invalid.requeue(), "invalid orders must not loop");
    }

    @Test
    void byKindRequeuesTransientFailures() {
        Rules.Decision transientFail = Rules.decide(
                Rules.OnFailure.BY_KIND,
                new EmailWorker.TransientFailure("mail server down"));
        assertFalse(transientFail.ack());
        assertTrue(transientFail.requeue());
    }

    @Test
    void onFailureParsingAcceptsTheThreePolicies() {
        assertEquals(Rules.OnFailure.REQUEUE, Rules.OnFailure.parse("requeue"));
        assertEquals(Rules.OnFailure.DISCARD, Rules.OnFailure.parse("discard"));
        assertEquals(Rules.OnFailure.BY_KIND, Rules.OnFailure.parse("by-kind"));
        assertThrows(IllegalArgumentException.class, () -> Rules.OnFailure.parse("retry"));
    }

    @Test
    void poisonMessageHasNoEmail() {
        OrderEvent poison = OrderEvent.of(2, true);
        assertFalse(poison.hasEmail());
        OrderEvent ok = OrderEvent.of(1, false);
        assertTrue(ok.hasEmail());
    }

    @Test
    void attemptsCountPerEventId() {
        Attempts attempts = new Attempts();
        assertEquals(1, attempts.next("evt_0001"));
        assertEquals(2, attempts.next("evt_0001"));
        assertEquals(1, attempts.next("evt_0002"));
    }

    @Test
    void reportLineIncludesAttemptRedeliveredAndDecision() {
        Rules.Decision decision = Rules.decide(
                Rules.OnFailure.REQUEUE,
                new EmailWorker.InvalidOrderException("order ord_1042 has no email address"));
        String line = Report.line("evt_0002", 3, true, "order ord_1042 has no email address", decision);
        assertEquals(
                "evt_0002 attempt 3, redelivered=true: order ord_1042 has no email address -> nack, requeue=true",
                line);
    }

    @Test
    void messageBodySurvivesAJsonRoundTrip() throws Exception {
        OrderEvent original = OrderEvent.of(4, false);
        assertEquals(original, EventJson.fromBytes(EventJson.toBytes(original)));
    }

    @Test
    void argsParseOnFailureAndNumbers() {
        Args args = new Args(new String[] {
                "consume", "--on-failure", "by-kind", "--work-ms", "100", "--limit", "2"
        }, 1);
        assertEquals("by-kind", args.text("on-failure", "requeue"));
        assertEquals(100L, args.number("work-ms", 250));
        assertEquals(2L, args.number("limit", 0));
    }
}
