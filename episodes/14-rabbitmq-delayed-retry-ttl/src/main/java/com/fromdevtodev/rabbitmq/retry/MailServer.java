package com.fromdevtodev.rabbitmq.retry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * A fake mail server for the demos: nothing is sent. The recipient's server of the events in {@code affected} is down for
 * {@code downMillis}, counted from the first call that reaches it. Every other call succeeds at once. The clock is a parameter so
 * that the window can be tested without waiting.
 */
final class MailServer {
    private final Set<String> affected;
    private final long downMillis;
    private final LongSupplier clock;
    private final Map<String, Integer> calls = new HashMap<>();
    private long downSince = -1;

    MailServer(Set<String> affected, long downMillis, LongSupplier clock) {
        this.affected = affected;
        this.downMillis = downMillis;
        this.clock = clock;
    }

    /** One call to the mail server. Returns the failure, or {@link Rules.Failure#NONE} when the email "went out". */
    synchronized Rules.Failure send(OrderEvent order) {
        calls.merge(order.eventId(), 1, Integer::sum);
        if (affected.contains(order.eventId())) {
            long now = clock.getAsLong();
            if (downSince < 0) {
                downSince = now;
            }
            if (now - downSince < downMillis) {
                return Rules.Failure.MAIL_SERVER_DOWN;
            }
        }
        return Rules.Failure.NONE;
    }

    /** How many calls the server has received for this event. */
    synchronized int callsFor(String eventId) {
        return calls.getOrDefault(eventId, 0);
    }
}
