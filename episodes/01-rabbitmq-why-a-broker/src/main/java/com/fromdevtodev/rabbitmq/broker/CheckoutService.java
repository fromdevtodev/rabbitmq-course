package com.fromdevtodev.rabbitmq.broker;

import java.io.IOException;

/**
 * The order service. A checkout saves the order, makes sure the confirmation email will be sent, and answers the customer.
 * The only difference between the two modes is <em>who waits for the email provider</em>:
 *
 * <ul>
 *   <li>{@link Mode#SYNC}: the service calls the provider itself and answers after it. It is the producer and the sender.</li>
 *   <li>{@link Mode#ASYNC}: the service is only a <em>producer</em>: it publishes an event and answers. A consumer sends the
 *       email later.</li>
 * </ul>
 */
public final class CheckoutService {
    /** Saving an order is a sleep, so every checkout has the same fixed cost whichever mode runs. */
    static final long SAVE_MILLIS = 40;

    /** What the customer experienced: how long the answer took and whether it was a success. */
    public record Answer(String orderId, boolean ok, long millis, String detail) { }

    private final Mode mode;
    private final EmailProvider provider;
    private final EventPublisher publisher;

    public CheckoutService(Mode mode, EmailProvider provider, EventPublisher publisher) {
        this.mode = mode;
        this.provider = provider;
        this.publisher = publisher;
    }

    // region readme:checkout
    public Answer checkout(OrderEvent order) {
        long start = System.nanoTime();
        pause(SAVE_MILLIS);
        Log.info("order-service", "order " + order.orderId() + " saved");
        try {
            switch (mode) {
                case SYNC -> {
                    Log.info("order-service", "calling the email provider and waiting for it ...");
                    provider.send(order.orderId());
                }
                case ASYNC -> {
                    // Publisher confirms are a different mechanism with their own episode: the call returns when the client
                    // library has handed the message to the connection, and the demo checks the queue separately.
                    publisher.publish(order);
                    Log.info("order-service", "published " + order.eventId() + " (" + order.type() + ")");
                }
            }
            return new Answer(order.orderId(), true, since(start), "ok");
        } catch (ProviderUnavailableException | IOException failure) {
            return new Answer(order.orderId(), false, since(start), failure.getMessage());
        }
    }
    // endregion

    private static long since(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
