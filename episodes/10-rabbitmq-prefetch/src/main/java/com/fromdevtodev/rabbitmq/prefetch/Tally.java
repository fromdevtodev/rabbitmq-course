package com.fromdevtodev.rabbitmq.prefetch;

import java.util.concurrent.atomic.AtomicLong;

/**
 * What one consumer knows about its own deliveries: how many it has received, and how many it has acknowledged.
 *
 * <p>The difference is the number of <em>in-flight messages</em>: delivered to this consumer, not yet acknowledged.
 * It is the same number the broker counts as unacknowledged for this consumer, as long as acknowledgements are sent
 * as soon as they are counted.
 */
final class Tally {
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong acknowledged = new AtomicLong();

    void received() {
        received.incrementAndGet();
    }

    void acknowledged() {
        acknowledged.incrementAndGet();
    }

    long receivedCount() {
        return received.get();
    }

    /** Messages this consumer holds right now: received, not yet acknowledged. */
    long inFlight() {
        return received.get() - acknowledged.get();
    }

    /** Messages this consumer has finished with and acknowledged. */
    long handled() {
        return acknowledged.get();
    }
}
