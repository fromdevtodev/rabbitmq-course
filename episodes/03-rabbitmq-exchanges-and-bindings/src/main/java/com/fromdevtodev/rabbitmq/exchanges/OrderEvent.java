package com.fromdevtodev.rabbitmq.exchanges;

import java.util.Locale;

/**
 * The message used in every demo: something happened to an order. The sequence number is baked into the event ID
 * ({@code evt_0001}, ...) so it is easy to follow one message from the publisher to every queue that received a copy.
 *
 * <p>The {@code type} is the same text as the routing key the message is published with ({@code order.created} or
 * {@code order.cancelled}). The broker never reads it: routing uses the key given to {@code basicPublish}, not the body.
 */
public record OrderEvent(String eventId, String orderId, String type) {
    public static final String CREATED = "order.created";
    public static final String CANCELLED = "order.cancelled";

    /** Message number {@code sequence} (1, 2, 3, ...) of the given type; the order is {@code ord_1001} for message 1. */
    public static OrderEvent of(long sequence, String type) {
        return new OrderEvent(
                String.format(Locale.ROOT, "evt_%04d", sequence),
                String.format(Locale.ROOT, "ord_%d", 1000 + sequence),
                type);
    }
}
