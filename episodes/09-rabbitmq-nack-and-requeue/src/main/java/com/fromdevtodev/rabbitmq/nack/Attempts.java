package com.fromdevtodev.rabbitmq.nack;

import java.util.HashMap;
import java.util.Map;

/**
 * Counts how many times <em>this process</em> has received each event, to make a loop visible in the log.
 *
 * <p>This is our own bookkeeping, kept in memory. RabbitMQ marks a redelivery with the <em>redelivered</em> flag, which
 * says "seen before", not how many times; and a restart of the worker starts the counts again from 1.
 */
final class Attempts {
    private final Map<String, Integer> counts = new HashMap<>();

    /** The attempt number of the delivery that just arrived: 1, then 2, then 3, ... */
    int next(String eventId) {
        return counts.merge(eventId, 1, Integer::sum);
    }
}
