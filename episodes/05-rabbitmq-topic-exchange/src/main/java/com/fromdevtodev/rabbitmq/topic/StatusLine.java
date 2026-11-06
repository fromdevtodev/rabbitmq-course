package com.fromdevtodev.rabbitmq.topic;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/** The one-line view of some queues used by the {@code status} command. */
final class StatusLine {
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss");

    private StatusLine() { }

    /** {@code ready} maps each queue to its ready messages, or to {@code null} if the queue does not exist yet. */
    static String format(LocalTime time, Map<String, Long> ready) {
        StringBuilder line = new StringBuilder(CLOCK.format(time));
        ready.forEach((queue, count) -> line.append(" | ").append(queue)
                .append(count == null ? " does not exist yet" : " ready " + count));
        return line.toString();
    }
}
