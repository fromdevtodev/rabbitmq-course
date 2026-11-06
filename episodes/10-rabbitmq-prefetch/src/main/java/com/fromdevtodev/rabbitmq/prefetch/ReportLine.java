package com.fromdevtodev.rabbitmq.prefetch;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** The one-line report each consumer prints once per interval. */
final class ReportLine {
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss");

    private ReportLine() { }

    static String format(LocalTime time, String consumerName, long inFlight, long handled) {
        return String.format(Locale.ROOT, "%s [%s] in flight %d | handled %d",
                CLOCK.format(time), consumerName, inFlight, handled);
    }
}
