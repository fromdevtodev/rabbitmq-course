package com.fromdevtodev.rabbitmq.direct;

import java.util.Locale;

/**
 * The message used in every demo: one log line. The sequence number is baked into the ID ({@code evt_0001}, ...) so
 * that it is easy to see which message reached which queue.
 *
 * <p>{@code level} repeats the routing key the message is published with ({@code error}, {@code warning}, ...). The
 * broker never reads the body: routing uses the routing key, not the JSON.
 */
public record LogEvent(String eventId, String level, String service) {
    public static final String SERVICE = "checkout";

    /** Message number {@code sequence} (1, 2, 3, ...) at the given level. */
    public static LogEvent of(long sequence, String level) {
        return new LogEvent(String.format(Locale.ROOT, "evt_%04d", sequence), level, SERVICE);
    }
}
