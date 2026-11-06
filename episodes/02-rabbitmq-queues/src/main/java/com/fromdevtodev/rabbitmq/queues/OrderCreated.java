package com.fromdevtodev.rabbitmq.queues;

import java.util.Locale;

/**
 * The message used in every demo. The sequence number is baked into the IDs
 * ({@code evt_0001}, {@code ord_1001}, ...) so that the order in which messages leave the queue is easy to read.
 */
public record OrderCreated(String eventId, String type, String orderId) {
    public static final String TYPE = "order.created";

    /** Message number {@code sequence} (1, 2, 3, ...). */
    public static OrderCreated of(long sequence) {
        return new OrderCreated(
                String.format(Locale.ROOT, "evt_%04d", sequence),
                TYPE,
                String.format(Locale.ROOT, "ord_%d", 1000 + sequence));
    }
}
