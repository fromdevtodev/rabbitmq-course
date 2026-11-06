package com.fromdevtodev.rabbitmq.ack;

import java.util.Locale;

/**
 * Where the demo worker stops abruptly. The worker halts the JVM (no clean shutdown, no channel close), which closes its
 * sockets the same way a killed process would: the broker only sees the connection disappear.
 */
enum CrashPoint {
    /** Never crashes. */
    NONE,
    /** Dies halfway through "sending the email": the work is not done, and no ack was sent. */
    WHILE_SENDING,
    /** Dies after the email was sent but before the ack: the work is done, and RabbitMQ cannot know it. */
    AFTER_SEND;

    /** Parses {@code none}, {@code while-sending} or {@code after-send}. */
    static CrashPoint parse(String text) {
        return switch (text.trim().toLowerCase(Locale.ROOT)) {
            case "none" -> NONE;
            case "while-sending" -> WHILE_SENDING;
            case "after-send" -> AFTER_SEND;
            default -> throw new IllegalArgumentException(
                    "unknown crash point '" + text + "': use none, while-sending or after-send");
        };
    }
}
