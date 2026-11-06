package com.fromdevtodev.rabbitmq.prefetch;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * The one-line view of a queue used by the {@code status} command.
 *
 * <p>The AMQP "passive declare" the status command relies on reports only the <em>ready</em> messages and the number
 * of consumers. In-flight (unacknowledged) messages are not part of that reply: each consumer reports its own, and
 * {@code rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers} shows the queue's total.
 */
final class StatusLine {
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss");

    private StatusLine() { }

    static String format(LocalTime time, String queue, long ready, long consumers) {
        return String.format(Locale.ROOT, "%s | %s | ready %d | consumers %d", CLOCK.format(time), queue, ready, consumers);
    }

    static String missing(LocalTime time, String queue) {
        return String.format(Locale.ROOT, "%s | %s | queue does not exist yet", CLOCK.format(time), queue);
    }
}
