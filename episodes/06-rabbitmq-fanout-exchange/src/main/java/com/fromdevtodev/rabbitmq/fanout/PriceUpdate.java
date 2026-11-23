package com.fromdevtodev.rabbitmq.fanout;

import java.util.Locale;

/**
 * The message used in every demo: one price change. The sequence number is baked into the event ID ({@code evt_0001}, ...) so it
 * is easy to follow one message from the publisher to every queue that received a copy.
 *
 * <p>The routing key is only a label here ({@code toys}, {@code books}, ...): a fanout exchange ignores it. The broker never
 * reads the body.
 */
public record PriceUpdate(String eventId, String routingKey, String summary) {

    /** Price change number {@code sequence} (1, 2, 3, ...), published with the given routing key. */
    public static PriceUpdate of(long sequence, String routingKey) {
        return new PriceUpdate(String.format(Locale.ROOT, "evt_%04d", sequence), routingKey, "price changed (" + routingKey + ")");
    }
}
