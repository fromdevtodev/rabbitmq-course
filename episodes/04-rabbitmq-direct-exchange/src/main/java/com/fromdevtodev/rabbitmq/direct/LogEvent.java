package com.fromdevtodev.rabbitmq.direct;

import java.util.Locale;

/**
 * The message used in every demo: one log line. The sequence number is baked into the event ID ({@code log_0001}, ...) so it
 * is easy to follow one message from the publisher to every queue that received a copy.
 *
 * <p>The {@code level} is the same text as the routing key the message is published with ({@code error}, {@code warning},
 * {@code info} or {@code debug}). The broker never reads it: routing uses the key given to {@code basicPublish}, not the body.
 */
public record LogEvent(String eventId, String level, String message) {
    public static final String ERROR = "error";
    public static final String WARNING = "warning";
    public static final String INFO = "info";
    public static final String DEBUG = "debug";

    /** Log line number {@code sequence} (1, 2, 3, ...) of the given level. */
    public static LogEvent of(long sequence, String level) {
        return new LogEvent(String.format(Locale.ROOT, "log_%04d", sequence), level, textFor(level));
    }

    private static String textFor(String level) {
        return switch (level) {
            case ERROR -> "payment gateway timeout";
            case WARNING -> "retrying in 5 seconds";
            case INFO -> "order 1002 shipped";
            case DEBUG -> "cache miss for order 1002";
            default -> "log line";
        };
    }
}
