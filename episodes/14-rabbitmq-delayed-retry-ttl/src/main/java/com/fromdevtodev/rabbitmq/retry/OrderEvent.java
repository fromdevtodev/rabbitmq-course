package com.fromdevtodev.rabbitmq.retry;

import java.util.Locale;

/**
 * The message used in every demo: an order that needs a confirmation email. The sequence number is baked into the IDs
 * ({@code evt_0001} / {@code ord_1041}, ...) so it is easy to follow one message through the log. The event ID is the identity of
 * the message; a delivery tag never is.
 */
public record OrderEvent(String eventId, String orderId, String customerEmail) {

    /** Event number {@code sequence} (1, 2, 3, ...): order {@code 1040 + sequence}. */
    public static OrderEvent of(long sequence) {
        long order = 1040 + sequence;
        return new OrderEvent(
                String.format(Locale.ROOT, "evt_%04d", sequence),
                "ord_" + order,
                "customer-" + order + "@example.com");
    }
}
