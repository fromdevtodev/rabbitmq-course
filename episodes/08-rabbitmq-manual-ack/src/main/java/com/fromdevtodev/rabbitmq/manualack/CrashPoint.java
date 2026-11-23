package com.fromdevtodev.rabbitmq.manualack;

import java.util.Locale;

/**
 * Where the demo worker dies. The crash is produced by halting the JVM: no acknowledgement, no {@code channel.close()}, no
 * {@code connection.close()}, which is what a killed process looks like to the broker.
 */
enum CrashPoint {
    /** The worker never crashes. */
    NONE,
    /** Halfway through sending the email: the work is not done. */
    WHILE_SENDING,
    /** After the email was sent and before the acknowledgement: the work is done, and the broker does not know. */
    AFTER_SEND;

    static CrashPoint parse(String text) {
        return switch (text.toLowerCase(Locale.ROOT)) {
            case "none" -> NONE;
            case "while-sending" -> WHILE_SENDING;
            case "after-send" -> AFTER_SEND;
            default -> throw new IllegalArgumentException("unknown --crash '" + text + "' (use none, while-sending or after-send)");
        };
    }
}
