package com.fromdevtodev.rabbitmq.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** No broker needed: the publisher is a list. */
class CheckoutTest {

    private static final class RecordingPublisher implements EventPublisher {
        final List<OrderEvent> published = new ArrayList<>();

        @Override
        public void publish(OrderEvent event) {
            published.add(event);
        }
    }

    @Test
    void syncCheckoutWaitsForTheProvider() {
        CheckoutService service = new CheckoutService(Mode.SYNC, new EmailProvider(300, false), new RecordingPublisher());
        CheckoutService.Answer answer = service.checkout(OrderEvent.of(1));
        assertTrue(answer.ok());
        assertTrue(answer.millis() >= 300, "the customer waited for the email: " + answer.millis());
    }

    // region readme:test
    @Test
    void asyncCheckoutAnswersWithoutWaitingAndPublishesTheEvent() {
        RecordingPublisher publisher = new RecordingPublisher();
        CheckoutService service = new CheckoutService(Mode.ASYNC, new EmailProvider(5_000, false), publisher);
        CheckoutService.Answer answer = service.checkout(OrderEvent.of(2));
        assertTrue(answer.ok());
        assertTrue(answer.millis() < 1_000, "the provider's latency is not the customer's: " + answer.millis());
        assertEquals(List.of(OrderEvent.of(2)), publisher.published);
    }
    // endregion

    @Test
    void aDownProviderFailsSyncCheckoutButNotAsyncCheckout() {
        EmailProvider down = new EmailProvider(2_000, true);
        CheckoutService.Answer sync = new CheckoutService(Mode.SYNC, down, new RecordingPublisher()).checkout(OrderEvent.of(1));
        assertFalse(sync.ok());
        assertTrue(sync.detail().contains("unavailable"));

        RecordingPublisher publisher = new RecordingPublisher();
        CheckoutService.Answer async = new CheckoutService(Mode.ASYNC, down, publisher).checkout(OrderEvent.of(1));
        assertTrue(async.ok());
        assertEquals(1, publisher.published.size());
    }

    @Test
    void aFailingPublisherIsAnErrorForTheCustomer() {
        EventPublisher broken = event -> { throw new IOException("broker unreachable"); };
        CheckoutService.Answer answer = new CheckoutService(Mode.ASYNC, new EmailProvider(0, false), broken).checkout(OrderEvent.of(1));
        assertFalse(answer.ok());
        assertEquals("broker unreachable", answer.detail());
    }

    @Test
    void providerIsDownOrTakesItsLatency() {
        assertThrows(ProviderUnavailableException.class, () -> new EmailProvider(0, true).send("ord_1001"));
        assertEquals("DOWN", new EmailProvider(0, true).describe());
        assertEquals("up, 2000 ms per email", new EmailProvider(2000, false).describe());
    }

    @Test
    void modesParseAndArgumentsDefault() {
        assertEquals(Mode.SYNC, Mode.parse(" Sync "));
        assertEquals(Mode.ASYNC, Mode.parse("async"));
        assertThrows(IllegalArgumentException.class, () -> Mode.parse("later"));
        Args args = new Args(new String[] {"checkout", "--mode", "async", "--orders", "8", "--interval-ms", "1000"}, 1);
        assertEquals("async", args.required("mode"));
        assertEquals(8L, args.number("orders", 1));
        assertEquals(1L, args.number("first", 1));
        assertEquals(2000L, args.number("provider-ms", 2000));
    }

    @Test
    void queueAndEventContractAreStable() throws IOException {
        assertEquals("notifications.email", Topology.QUEUE);
        assertEquals("order.created", OrderEvent.of(1).type());
        assertEquals("evt_0001", OrderEvent.of(1).eventId());
        assertEquals("ord_1001", OrderEvent.of(1).orderId());
        assertEquals(OrderEvent.of(3), EventJson.fromBytes(EventJson.toBytes(OrderEvent.of(3))));
    }
}
