package com.fromdevtodev.rabbitmq.exchanges;

import java.util.Locale;

/**
 * The message used in every demo. The sequence number is baked into the IDs
 * ({@code evt_0001}, {@code ord_1001}, ...) so that it is easy to see which message reached which queue.
 *
 * <p>{@code type} repeats the routing key the message is published with ({@code order.created} or
 * {@code order.cancelled}). The broker never reads the body: routing uses the routing key, not the JSON.
 */
public record OrderEvent(String eventId, String type, String orderId) {

    /** Message number {@code sequence} (1, 2, 3, ...) of the given type. */
    public static OrderEvent of(long sequence, String type) {
        return new OrderEvent(
                String.format(Locale.ROOT, "evt_%04d", sequence),
                type,
                String.format(Locale.ROOT, "ord_%d", 1000 + sequence));
    }
}
