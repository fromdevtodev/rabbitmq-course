package com.fromdevtodev.rabbitmq.whyabroker;

import java.util.function.BooleanSupplier;

/** Tiny polling helper for conditions that become true "later", which is the whole point of this episode. */
final class Eventually {
    private Eventually() { }

    static boolean within(long millis, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + millis * 1_000_000;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return condition.getAsBoolean();
    }
}
