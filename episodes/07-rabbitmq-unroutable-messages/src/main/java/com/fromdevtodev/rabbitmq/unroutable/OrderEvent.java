package com.fromdevtodev.rabbitmq.unroutable;

import java.util.Locale;

/**
 * The message used in every demo: one order event. The sequence number is baked into the ID ({@code evt_0001}, ...) so
 * that it is easy to see which message reached which queue, or came back.
 *
 * <p>{@code type} repeats the routing key the message is published with. The broker never reads the body: routing
 * uses the routing key, not the JSON.
 */
public record OrderEvent(String eventId, String type, String orderId) {
    public static final String ORDER_ID = "ord_1042";

    /** Message number {@code sequence} (1, 2, 3, ...) published with the routing key {@code type}. */
    public static OrderEvent of(long sequence, String type) {
        return new OrderEvent(String.format(Locale.ROOT, "evt_%04d", sequence), type, ORDER_ID);
    }
}
