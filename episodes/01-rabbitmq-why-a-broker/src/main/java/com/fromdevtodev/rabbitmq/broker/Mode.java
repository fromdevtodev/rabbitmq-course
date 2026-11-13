package com.fromdevtodev.rabbitmq.broker;

import java.util.Locale;

/** How the order service sends the confirmation email. */
public enum Mode {
    /** Call the email provider and wait for it before answering the customer. */
    SYNC,
    /** Publish an {@code order.created} event to the broker and answer the customer. */
    ASYNC;

    public static Mode parse(String text) {
        return switch (text.trim().toLowerCase(Locale.ROOT)) {
            case "sync" -> SYNC;
            case "async" -> ASYNC;
            default -> throw new IllegalArgumentException("unknown mode '" + text + "' (use sync or async)");
        };
    }
}
