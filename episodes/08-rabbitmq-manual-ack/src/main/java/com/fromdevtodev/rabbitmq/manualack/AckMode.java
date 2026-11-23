package com.fromdevtodev.rabbitmq.manualack;

import java.util.Locale;

/**
 * How a consumer tells the broker that a delivery is done. In {@link #AUTO} the broker considers a message handled the moment
 * it sends it; in {@link #MANUAL} the broker keeps the message, counted as unacknowledged, until the consumer answers.
 */
enum AckMode {
    AUTO("automatic", true),
    MANUAL("manual", false);

    private final String label;
    private final boolean autoAck;

    AckMode(String label, boolean autoAck) {
        this.label = label;
        this.autoAck = autoAck;
    }

    /** The {@code autoAck} flag of {@code basicConsume}. */
    boolean autoAck() {
        return autoAck;
    }

    String label() {
        return label;
    }

    static AckMode parse(String text) {
        return switch (text.toLowerCase(Locale.ROOT)) {
            case "auto", "automatic" -> AUTO;
            case "manual" -> MANUAL;
            default -> throw new IllegalArgumentException("unknown --mode '" + text + "' (use auto or manual)");
        };
    }
}
