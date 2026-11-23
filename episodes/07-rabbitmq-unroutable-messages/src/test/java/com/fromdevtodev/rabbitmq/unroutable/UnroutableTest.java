package com.fromdevtodev.rabbitmq.unroutable;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import org.junit.jupiter.api.Test;

/** No broker needed: these cover the rule and the small pieces of the project that do not talk to RabbitMQ. */
class UnroutableTest {

    // region readme:test
    @Test
    void aKeyWithABindingReachesEveryQueueBoundWithIt() {
        assertEquals(Set.of("notifications.email", "loyalty.points", "audit.log"), DirectRule.queuesFor("order.created", Topology.ORDERS));
        assertEquals(Set.of("notifications.email", "audit.log"), DirectRule.queuesFor("order.cancelled", Topology.ORDERS));
    }

    @Test
    void oneLetterOfDifferenceMakesTheMessageUnroutable() {
        assertTrue(DirectRule.isUnroutable("order.canceled", Topology.ORDERS));      // one L: no binding has this key
        assertFalse(DirectRule.isUnroutable("order.cancelled", Topology.ORDERS));    // two Ls: bound
        assertTrue(DirectRule.isUnroutable("Order.created", Topology.ORDERS));       // keys are exact, case included
    }
    // endregion

    @Test
    void theEventIdAndTheOrderIdCarryTheSequenceNumber() {
        OrderEvent event = OrderEvent.of(2, "order.canceled");
        assertEquals("evt_0002", event.eventId());
        assertEquals("ord_1002", event.orderId());
    }

    @Test
    void argumentsAreParsedAsNameValuePairs() {
        Args args = new Args(new String[] {"publish", "--key", "order.canceled", "--mandatory", "true"}, 1);
        assertEquals("order.canceled", args.text("key", "x"));
        assertTrue(Boolean.parseBoolean(args.text("mandatory", "false")));
        assertEquals(1, args.number("first", 1));
    }
}
