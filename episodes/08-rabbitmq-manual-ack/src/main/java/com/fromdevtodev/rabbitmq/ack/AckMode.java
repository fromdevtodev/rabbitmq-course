package com.fromdevtodev.rabbitmq.ack;

import java.util.Locale;

/**
 * The two acknowledgement modes a consumer can ask for.
 *
 * <p>In {@link #AUTOMATIC} mode RabbitMQ counts a message as handled as soon as it has been delivered: the consumer never
 * answers. In {@link #MANUAL} mode the message stays <em>unacknowledged</em> until the consumer calls {@code basicAck}.
 */
enum AckMode {
    AUTOMATIC(true),
    MANUAL(false);

    private final boolean autoAck;

    AckMode(boolean autoAck) {
        this.autoAck = autoAck;
    }

    /** The {@code autoAck} argument of {@code basicConsume}. */
    boolean autoAck() {
        return autoAck;
    }

    /** Parses {@code auto} or {@code manual} (any case). */
    static AckMode parse(String text) {
        return switch (text.trim().toLowerCase(Locale.ROOT)) {
            case "auto", "automatic" -> AUTOMATIC;
            case "manual" -> MANUAL;
            default -> throw new IllegalArgumentException("unknown mode '" + text + "': use auto or manual");
        };
    }
}
