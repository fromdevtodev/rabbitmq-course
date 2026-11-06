package com.fromdevtodev.rabbitmq.whyabroker;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stand-in for the database commit in the video.
 *
 * <p>Orders are not stored anywhere: {@link #save} only waits for {@code commitLatency}
 * (40 ms by default) and hands out the next ID. The point is the time the caller spends, not the storage.
 */
public final class OrderRepository {
    private final AtomicLong sequence = new AtomicLong(1041); // the first order is ord_1042
    private final Duration commitLatency;

    public OrderRepository(Duration commitLatency) {
        this.commitLatency = commitLatency;
    }

    public Order save(String customerId) throws InterruptedException {
        Thread.sleep(commitLatency);
        return new Order("ord_" + sequence.incrementAndGet(), customerId);
    }
}
