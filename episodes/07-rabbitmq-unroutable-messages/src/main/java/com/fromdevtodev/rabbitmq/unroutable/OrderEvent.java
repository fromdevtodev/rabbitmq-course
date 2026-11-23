package com.fromdevtodev.rabbitmq.unroutable;

import java.util.Locale;

/**
 * The message used in every demo: one order event. The sequence number is baked into the event ID ({@code evt_0001}, ...) and the
 * order ID ({@code ord_1001}, ...) so that it is easy to follow one message from the publisher to a queue, or back to the publisher.
 * The broker never reads the body: routing uses the key given to {@code basicPublish}.
 */
public record OrderEvent(String eventId, String orderId, String routingKey) {

    /** Event number {@code sequence} (1, 2, 3, ...), published with the given routing key. */
    public static OrderEvent of(long sequence, String routingKey) {
        return new OrderEvent(String.format(Locale.ROOT, "evt_%04d", sequence), "ord_" + (1000 + sequence), routingKey);
    }
}
