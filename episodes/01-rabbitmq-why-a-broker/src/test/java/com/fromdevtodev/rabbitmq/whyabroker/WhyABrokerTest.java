package com.fromdevtodev.rabbitmq.whyabroker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Executable versions of the claims in the episode. Timings are scaled down (a 500 ms provider instead of 7.9 s)
 * and checked with wide margins. The broker is an in-memory stand-in, so these tests do not need RabbitMQ.
 */
class WhyABrokerTest {
    private static final long PROVIDER_MS = 500;

    private static OrderService service(OrderFollowUp followUp) {
        return new OrderService(new OrderRepository(Duration.ofMillis(10)), followUp);
    }

    private static HttpEmailClient emailClient(FakeEmailProvider provider, long timeoutMillis) {
        return new HttpEmailClient(URI.create("http://127.0.0.1:" + provider.port()), Duration.ofMillis(timeoutMillis));
    }

    private static long millis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    @Test
    void inlineCallMakesTheCustomerWaitForTheProvider() throws Exception {
        try (FakeEmailProvider provider = FakeEmailProvider.start(0, PROVIDER_MS)) {
            OrderService orders = service(emailClient(provider, 5_000)::sendConfirmation);

            long start = System.nanoTime();
            Order order = orders.place("cus_88");

            assertTrue(millis(start) >= PROVIDER_MS, "the caller's wait includes the provider's wait");
            assertEquals(1, provider.acceptedFor(order.id()));
        }
    }

    @Test
    void publishingLetsTheCallerAnswerNowAndTheEmailGoesOutLater() throws Exception {
        try (FakeEmailProvider provider = FakeEmailProvider.start(0, PROVIDER_MS)) {
            InMemoryBroker broker = new InMemoryBroker();
            OrderService orders = service(broker::orderCreated);
            Thread consumer = broker.startConsumer(new EmailJob(emailClient(provider, 5_000), Duration.ofMillis(50)));

            long start = System.nanoTime();
            Order order = orders.place("cus_88");
            long callerWait = millis(start);

            assertTrue(callerWait < PROVIDER_MS / 2, "the caller did not wait for the provider (waited " + callerWait + " ms)");
            assertEquals(0, provider.acceptedFor(order.id()), "the email has not been sent yet");
            assertTrue(Eventually.within(5_000, () -> provider.acceptedFor(order.id()) == 1), "the email goes out later");
            consumer.interrupt();
        }
    }

    @Test
    void providerDownFailsTheInlineCheckoutEvenThoughTheOrderIsSaved() throws Exception {
        try (FakeEmailProvider provider = FakeEmailProvider.start(0, PROVIDER_MS)) {
            provider.setDown(true);
            OrderService orders = service(emailClient(provider, 5_000)::sendConfirmation);

            FollowUpFailedException failure = assertThrows(FollowUpFailedException.class, () -> orders.place("cus_88"));

            assertTrue(failure.order().id().startsWith("ord_"), "the order exists");
            assertEquals(0, provider.acceptedTotal());
        }
    }

    @Test
    void providerDownDoesNotFailTheCheckoutWhenTheWorkIsPublished() throws Exception {
        try (FakeEmailProvider provider = FakeEmailProvider.start(0, PROVIDER_MS)) {
            provider.setDown(true);
            InMemoryBroker broker = new InMemoryBroker();
            OrderService orders = service(broker::orderCreated);
            Thread consumer = broker.startConsumer(new EmailJob(emailClient(provider, 5_000), Duration.ofMillis(50)));

            Order first = orders.place("cus_88");
            Order second = orders.place("cus_89"); // both succeed while the provider is down

            assertEquals(0, provider.acceptedTotal());
            assertTrue(Eventually.within(1_000, () -> broker.waitingCount() >= 1), "the backlog is in the broker, not on the customer's screen");

            provider.setDown(false);
            assertTrue(Eventually.within(10_000, () -> provider.acceptedFor(first.id()) == 1 && provider.acceptedFor(second.id()) == 1),
                    "when the provider is back the backlog drains");
            consumer.interrupt();
        }
    }

    @Test
    void aTimeoutIsAnUnknownOutcomeNotAFailure() throws Exception {
        try (FakeEmailProvider provider = FakeEmailProvider.start(0, 600)) {
            OrderService orders = service(emailClient(provider, 200)::sendConfirmation); // gives up after 200 ms

            FollowUpFailedException failure = assertThrows(FollowUpFailedException.class, () -> orders.place("cus_88"));

            // The caller saw a failure, yet the provider accepted the email anyway.
            assertTrue(Eventually.within(3_000, () -> provider.acceptedFor(failure.order().id()) == 1),
                    "the provider did the work although the caller timed out");
        }
    }

    @Test
    void theSameMessageHandledTwiceSendsTheEmailTwice() throws Exception {
        try (FakeEmailProvider provider = FakeEmailProvider.start(0, 10)) {
            EmailJob job = new EmailJob(emailClient(provider, 5_000), Duration.ofMillis(50));
            OrderCreated event = OrderCreated.from(new Order("ord_1042", "cus_88"));

            job.handle(event);
            job.handle(event); // what a redelivery looks like to the consumer

            assertEquals(2, provider.acceptedFor("ord_1042"), "a duplicate delivery is a duplicate email unless the consumer copes with it");
        }
    }

    @Test
    void waitingRequestsHoldThreadsAndCapacityRunsOut() throws Exception {
        try (FakeEmailProvider provider = FakeEmailProvider.start(0, 400);
             OrderServer server = OrderServer.start(0, 2, service(emailClient(provider, 5_000)::sendConfirmation))) {
            java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + server.port() + "/orders")).POST(java.net.http.HttpRequest.BodyPublishers.noBody()).build();

            long start = System.nanoTime();
            var responses = java.util.stream.IntStream.range(0, 4)
                    .mapToObj(i -> http.sendAsync(request, java.net.http.HttpResponse.BodyHandlers.discarding())).toList();
            for (var response : responses) {
                assertEquals(201, response.get().statusCode());
            }

            // 4 slow requests on 2 threads need two "rounds" of the provider's 400 ms.
            assertTrue(millis(start) >= 800, "the last customers waited for a free thread (total " + millis(start) + " ms)");
        }
    }
}
