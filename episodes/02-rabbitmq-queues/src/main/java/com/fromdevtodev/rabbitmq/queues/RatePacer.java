package com.fromdevtodev.rabbitmq.queues;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Lets at most {@code perSecond} events through per second, on average.
 *
 * <p>It keeps a schedule ("event number n is due at start + n / rate") instead of sleeping a fixed time after each
 * event, so small delays do not accumulate: if one event is late, the next ones catch up. That is how the demos get
 * a steady "100 messages per second in, 60 per second out".
 */
public final class RatePacer {
    /** How the pacer waits. Replaced by a fake in the tests, so they do not depend on real time. */
    public interface Sleeper {
        void sleepNanos(long nanos) throws InterruptedException;
    }

    private final long intervalNanos;
    private final LongSupplier nanoClock;
    private final Sleeper sleeper;
    private long dueNanos;
    private boolean started;

    /** {@code perSecond <= 0} means no limit: {@link #pace()} returns at once. */
    public RatePacer(double perSecond) {
        this(perSecond, System::nanoTime, nanos -> TimeUnit.NANOSECONDS.sleep(nanos));
    }

    RatePacer(double perSecond, LongSupplier nanoClock, Sleeper sleeper) {
        this.intervalNanos = perSecond > 0 ? (long) (1_000_000_000d / perSecond) : 0;
        this.nanoClock = nanoClock;
        this.sleeper = sleeper;
    }

    /** Blocks until it is this event's turn. The first event goes through immediately. */
    public void pace() throws InterruptedException {
        if (intervalNanos == 0) {
            return;
        }
        long now = nanoClock.getAsLong();
        if (!started) {
            started = true;
            dueNanos = now;
        }
        long wait = dueNanos - now;
        if (wait > 0) {
            sleeper.sleepNanos(wait);
        }
        dueNanos += intervalNanos;
    }
}
