package com.fromdevtodev.rabbitmq.confirms;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the tracker, the rule and the small pieces of the project that do not talk to RabbitMQ. */
class ConfirmsTest {

    // region readme:test
    @Test
    void aConfirmAnswersOnlyTheMessageItNames() {
        ConfirmTracker tracker = new ConfirmTracker();
        tracker.register(1, OrderEvent.of(1));
        tracker.register(2, OrderEvent.of(2));

        List<ConfirmTracker.Resolved> resolved = tracker.confirmed(1, false);

        assertEquals(1, resolved.size());
        assertEquals(List.of(2L), tracker.unanswered());
    }

    @Test
    void anAnswerWithMultipleCoversEveryLowerNumber() {
        ConfirmTracker tracker = new ConfirmTracker();
        for (long n = 1; n <= 3; n++) {
            tracker.register(n, OrderEvent.of(n));
        }

        List<ConfirmTracker.Resolved> resolved = tracker.confirmed(3, true);

        assertEquals(3, resolved.size());
        assertTrue(tracker.unanswered().isEmpty());
    }

    @Test
    void aNackResolvesTheMessageAsNotTaken() {
        ConfirmTracker tracker = new ConfirmTracker();
        CompletableFuture<ConfirmTracker.Answer> answer = tracker.register(3, OrderEvent.of(3));

        tracker.nacked(3, false);

        assertEquals(ConfirmTracker.Answer.NACKED, answer.getNow(null));
    }

    @Test
    void silenceIsNotAnAnswer() {
        ConfirmTracker tracker = new ConfirmTracker();
        CompletableFuture<ConfirmTracker.Answer> answer = tracker.register(2, OrderEvent.of(2));

        assertFalse(answer.isDone());
        assertEquals(List.of(2L), tracker.unanswered());
    }
    // endregion

    @Test
    void answersMayArriveInAnyOrder() {
        ConfirmTracker tracker = new ConfirmTracker();
        for (long n = 1; n <= 3; n++) {
            tracker.register(n, OrderEvent.of(n));
        }

        tracker.confirmed(3, false);
        tracker.confirmed(1, false);

        assertEquals(List.of(2L), tracker.unanswered());
    }

    @Test
    void anAnswerForTheSameNumberTwiceResolvesItOnce() {
        ConfirmTracker tracker = new ConfirmTracker();
        tracker.register(1, OrderEvent.of(1));

        assertEquals(1, tracker.confirmed(1, true).size());
        assertEquals(0, tracker.confirmed(1, true).size());
    }

    @Test
    void aRepublishKeepsTheEventIdAndTakesANewSequenceNumber() {
        ConfirmTracker tracker = new ConfirmTracker();
        tracker.register(2, OrderEvent.of(2));
        tracker.register(3, OrderEvent.of(2));

        List<ConfirmTracker.Resolved> resolved = tracker.confirmed(3, true);

        assertEquals(List.of(2L, 3L), resolved.stream().map(ConfirmTracker.Resolved::sequence).toList());
        assertEquals(1, resolved.stream().map(r -> r.event().eventId()).distinct().count());
    }

    @Test
    void whatYouKnowAfterEachAnswer() {
        assertEquals(ConfirmRule.Verdict.TAKEN, ConfirmRule.whatYouKnow(ConfirmRule.Heard.CONFIRM));
        assertEquals(ConfirmRule.Verdict.REFUSED, ConfirmRule.whatYouKnow(ConfirmRule.Heard.NACK));
        assertEquals(ConfirmRule.Verdict.UNKNOWN, ConfirmRule.whatYouKnow(ConfirmRule.Heard.SILENCE));
    }

    @Test
    void aConfirmSaysNothingAboutAConsumer() {
        assertTrue(ConfirmRule.Verdict.TAKEN.knowledge.contains("no consumer has processed it"));
    }

    @Test
    void messagesAreNumberedInPublishingOrder() {
        assertEquals("evt_0001", OrderEvent.of(1).eventId());
        assertEquals("ord_1002", OrderEvent.of(2).orderId());
    }

    @Test
    void theEventSurvivesTheTripThroughBytes() throws IOException {
        OrderEvent event = OrderEvent.of(2);
        assertEquals(event, EventJson.fromBytes(EventJson.toBytes(event)));
    }

    @Test
    void optionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"publish", "--count", "3", "--pace", "burst"}, 1);
        assertEquals(3, args.number("count", 1));
        assertEquals("burst", args.text("pace", "one-by-one"));
        assertFalse(args.has("wait-for-file"));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--count"}, 1));
    }
}
