package com.fromdevtodev.rabbitmq.ack;

import java.util.Locale;

/**
 * The message used in every demo: an order was created and a confirmation email must be sent. The sequence number is
 * baked into the event ID ({@code evt_0001}, ...) so it is easy to follow one message across publisher, worker and queue.
 *
 * <p>The event ID identifies the <em>message</em>. The broker's delivery number does not: it only identifies one delivery on
 * one channel.
 */
public record OrderEvent(String eventId, String orderId, String type) {
    public static final String TYPE = "order.created";

    /** Message number {@code sequence} (1, 2, 3, ...); the order is {@code ord_1001} for message 1, and so on. */
    public static OrderEvent of(long sequence) {
        return new OrderEvent(
                String.format(Locale.ROOT, "evt_%04d", sequence),
                String.format(Locale.ROOT, "ord_%d", 1000 + sequence),
                TYPE);
    }
}
