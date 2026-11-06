package com.fromdevtodev.rabbitmq.queues;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/** The one-line view of a queue used by the {@code status} command. */
final class StatusLine {
    private StatusLine() { }

    static String format(LocalTime time, String queue, long ready, long consumers) {
        return String.format(Locale.ROOT, "%s | %s | ready %d | consumers %d",
                time.truncatedTo(ChronoUnit.SECONDS), queue, ready, consumers);
    }

    static String missing(LocalTime time, String queue) {
        return String.format(Locale.ROOT, "%s | %s | queue does not exist yet",
                time.truncatedTo(ChronoUnit.SECONDS), queue);
    }
}
