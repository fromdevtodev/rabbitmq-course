package com.fromdevtodev.rabbitmq.topic;

import java.util.Locale;

/**
 * The message used in every demo: one business event. The sequence number is baked into the event ID ({@code evt_0001}, ...)
 * so it is easy to follow one message from the publisher to every queue that received a copy.
 *
 * <p>The routing key is a list of words separated by dots, such as {@code orders.eu.created}: the domain, the region and what
 * happened. The broker never reads the body: routing uses the key given to {@code basicPublish}.
 */
public record DomainEvent(String eventId, String routingKey, String summary) {

    /** Event number {@code sequence} (1, 2, 3, ...), published with the given routing key. */
    public static DomainEvent of(long sequence, String routingKey) {
        return new DomainEvent(String.format(Locale.ROOT, "evt_%04d", sequence), routingKey, routingKey.replace('.', ' '));
    }
}
