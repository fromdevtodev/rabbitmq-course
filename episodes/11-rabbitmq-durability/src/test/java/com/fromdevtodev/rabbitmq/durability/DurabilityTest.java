package com.fromdevtodev.rabbitmq.durability;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the rule and the small pieces of the project that do not talk to RabbitMQ. */
class DurabilityTest {

    // region readme:test
    @Test
    void aDurableQueueWithPersistentMessagesKeepsBoth() {
        assertEquals(RestartRule.Outcome.QUEUE_AND_MESSAGES, RestartRule.afterRestart(true, true));
    }

    @Test
    void aDurableQueueWithTransientMessagesComesBackEmpty() {
        assertEquals(RestartRule.Outcome.QUEUE_ONLY, RestartRule.afterRestart(true, false));
    }

    @Test
    void aQueueThatIsNotDurableIsGoneWhateverItHeld() {
        assertEquals(RestartRule.Outcome.NOTHING, RestartRule.afterRestart(false, true));
        assertEquals(RestartRule.Outcome.NOTHING, RestartRule.afterRestart(false, false));
    }
    // endregion

    @Test
    void messagesAreNumberedInPublishingOrder() {
        assertEquals("evt_0001", OrderEvent.of(1).eventId());
        assertEquals("ord_1001", OrderEvent.of(1).orderId());
        assertEquals("evt_0006", OrderEvent.of(6).eventId());
    }

    @Test
    void theEventSurvivesTheTripThroughBytes() throws IOException {
        OrderEvent event = OrderEvent.of(4);
        assertEquals(event, EventJson.fromBytes(EventJson.toBytes(event)));
    }

    @Test
    void namesMatchTheEpisode() {
        assertEquals("notifications.email", Topology.EMAIL);
        assertEquals("loyalty.points", Topology.POINTS);
        assertEquals(2, Topology.QUEUES.size());
    }

    @Test
    void optionsAreParsedAndDefaulted() {
        Args args = new Args(new String[] {"publish", "--queue", "loyalty.points", "--persistent", "true"}, 1);
        assertEquals("loyalty.points", args.text("queue", Topology.EMAIL));
        assertTrue(args.flag("persistent", false));
        assertFalse(args.flag("durable", false));
        assertEquals(1, args.number("count", 1));
    }

    @Test
    void anOptionWithoutAValueIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Args(new String[] {"publish", "--count"}, 1));
    }
}
