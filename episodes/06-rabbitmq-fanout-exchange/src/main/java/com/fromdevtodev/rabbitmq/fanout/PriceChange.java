package com.fromdevtodev.rabbitmq.fanout;

import java.util.Locale;

/**
 * The message used in every demo: "a price changed". The sequence number is baked into the ID ({@code evt_0001}, ...) so
 * that it is easy to see which message reached which queue.
 *
 * <p>{@code category} repeats the routing key the message is published with ({@code books}, {@code toys}, ...). A fanout
 * exchange never reads it: routing ignores the routing key, and the broker never reads the JSON body at all.
 */
public record PriceChange(String eventId, String category, String service) {
    public static final String SERVICE = "pricing-service";

    /** Message number {@code sequence} (1, 2, 3, ...) in the given category. */
    public static PriceChange of(long sequence, String category) {
        return new PriceChange(String.format(Locale.ROOT, "evt_%04d", sequence), category, SERVICE);
    }
}
