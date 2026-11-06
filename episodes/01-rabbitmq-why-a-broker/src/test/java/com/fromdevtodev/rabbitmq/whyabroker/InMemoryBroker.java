package com.fromdevtodev.rabbitmq.whyabroker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A stand-in for the broker, used only by the tests so they run without RabbitMQ.
 *
 * <p>It models just what this episode claims: publishing returns immediately, and the message waits until a
 * consumer takes it. It says nothing about how RabbitMQ really behaves; that is what the manual run in the README is for.
 */
final class InMemoryBroker implements OrderEventPublisher {
    private final BlockingQueue<OrderCreated> waiting = new LinkedBlockingQueue<>();

    @Override
    public void orderCreated(Order order) {
        waiting.add(OrderCreated.from(order));
    }

    int waitingCount() {
        return waiting.size();
    }

    /** Starts a consumer thread that runs {@code job} for each message, one at a time. */
    Thread startConsumer(EmailJob job) {
        Thread consumer = Thread.ofPlatform().daemon().start(() -> {
            try {
                while (true) {
                    job.handle(waiting.take());
                }
            } catch (InterruptedException stopped) {
                // test finished
            }
        });
        return consumer;
    }
}
