package com.fromdevtodev.rabbitmq.queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The consumer: takes messages from the head of the queue, one after the other, at a steady rate.
 *
 * <p>"Processing" a message is just waiting for its turn in the {@link RatePacer}. The message is acknowledged
 * when that turn comes. Acknowledgements are a later episode; here they are plumbing, and
 * {@code prefetch 1} means the broker hands over the next message only after the previous one is done.
 */
final class Consumer {
    private Consumer() { }

    static void run(double perSecond, int prefetch, long logEvery) throws IOException, TimeoutException, InterruptedException {
        RatePacer pacer = new RatePacer(perSecond);
        AtomicLong handled = new AtomicLong();
        CountDownLatch stopped = new CountDownLatch(1);
        try (Connection connection = Topology.connect("consumer");
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            channel.basicQos(prefetch);
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                long tag = delivery.getEnvelope().getDeliveryTag();
                OrderCreated event = EventJson.fromBytes(delivery.getBody());
                try {
                    pacer.pace();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stopped.countDown();
                    return; // not acknowledged: the message stays in the queue
                }
                long n = handled.incrementAndGet();
                if (logEvery > 0 && n % logEvery == 0) {
                    Log.info("consumer", "handled #" + n + ": " + event.eventId() + " (" + event.orderId() + ")");
                }
                channel.basicAck(tag, false);
            };
            channel.basicConsume(Topology.QUEUE, false, onMessage, consumerTag -> stopped.countDown());
            Log.info("consumer", "waiting for messages on '" + Topology.QUEUE + "' at "
                    + (perSecond > 0 ? perSecond + " per second" : "full speed") + " (Ctrl+C to stop)");
            stopped.await();
        }
    }
}
