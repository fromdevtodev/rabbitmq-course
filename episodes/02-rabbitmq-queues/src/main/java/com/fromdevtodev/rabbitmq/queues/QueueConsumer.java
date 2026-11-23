package com.fromdevtodev.rabbitmq.queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A consumer of {@code notifications.email}: it takes the messages one at a time, in the order the queue gives them, and
 * "handles" each for {@code delayMillis} (nothing is really sent: handling is a pause and a log line).
 *
 * <p>Two settings here belong to later episodes and are not explained in this one: the consumer acknowledges each message after
 * it handled it (episode 8), and it takes one message at a time (prefetch, episode 10). They are what makes the queue's
 * numbers easy to read.
 */
final class QueueConsumer {
    private QueueConsumer() { }

    /** Consumes until Ctrl+C, or until {@code limit} messages were handled ({@code limit <= 0}: no limit). */
    static void run(String name, long delayMillis, long limit) throws IOException, TimeoutException, InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicLong received = new AtomicLong();  // one message at a time (basicQos 1), so no two callbacks overlap
        try (Connection connection = Topology.connect(name);
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel);
            // region readme:consumer
            channel.basicQos(1);
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                OrderEvent event = EventJson.fromBytes(delivery.getBody());
                long position = received.incrementAndGet();
                Log.info(name, "received " + event.eventId() + " (" + event.orderId() + "), message " + position + " for this consumer");
                sleep(delayMillis);
                Log.info(name, "handled " + event.eventId());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                if (limit > 0 && position >= limit) {
                    stopped.countDown();
                }
            };
            channel.basicConsume(Topology.QUEUE, false, onMessage, consumerTag -> stopped.countDown());
            // endregion
            Log.info(name, "waiting for messages on '" + Topology.QUEUE + "' (Ctrl+C to stop)");
            stopped.await();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(Math.max(0, millis));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
