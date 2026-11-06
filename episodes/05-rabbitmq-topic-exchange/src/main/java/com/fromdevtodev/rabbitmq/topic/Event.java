package com.fromdevtodev.rabbitmq.topic;

import java.util.Locale;

/**
 * The message used in every demo: one business event. The sequence number is baked into the ID ({@code evt_0001}, ...)
 * so that it is easy to see which message reached which queue.
 *
 * <p>{@code type} repeats the routing key the message is published with ({@code orders.eu.created}, ...). The broker never
 * reads the body: routing uses the routing key, not the JSON.
 */
public record Event(String eventId, String type) {

    /** Message number {@code sequence} (1, 2, 3, ...), published with the routing key {@code routingKey}. */
    public static Event of(long sequence, String routingKey) {
        return new Event(String.format(Locale.ROOT, "evt_%04d", sequence), routingKey);
    }
}
