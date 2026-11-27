package com.fromdevtodev.rabbitmq.retry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * How many times THIS worker has received each message. RabbitMQ keeps no such counter for a classic queue: it only sets the
 * redelivered flag, a hint that the message may have been delivered before. The number in the log comes from here, not from the broker.
 */
final class Attempts {
    private final Map<String, Integer> counts = new ConcurrentHashMap<>();

    /** Records one more delivery of the message and returns the new count (1 for the first delivery). */
    int next(String eventId) {
        return counts.merge(eventId, 1, Integer::sum);
    }

    int of(String eventId) {
        return counts.getOrDefault(eventId, 0);
    }
}
