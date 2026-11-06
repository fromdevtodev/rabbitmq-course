package com.fromdevtodev.rabbitmq.queues;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer: puts numbered messages at the tail of the queue, optionally at a steady rate.
 *
 * <p>It does not wait for a consumer and does not ask the broker to confirm each message (publisher confirms are a
 * later episode).
 */
final class Publisher {
    private Publisher() { }

    /** Publishes messages {@code first .. first + count - 1}; {@code perSecond <= 0} means as fast as possible. */
    static void run(long count, long first, double perSecond) throws IOException, TimeoutException, InterruptedException {
        RatePacer pacer = new RatePacer(perSecond);
        try (Connection connection = Topology.connect("publisher");
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            long started = System.nanoTime();
            for (long seq = first; seq < first + count; seq++) {
                pacer.pace();
                OrderCreated event = OrderCreated.of(seq);
                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType("application/json")
                        .messageId(event.eventId())
                        .deliveryMode(2)
                        .build();
                channel.basicPublish("", Topology.QUEUE, properties, EventJson.toBytes(event));
                if (count <= 10) {
                    Log.info("publisher", "published " + event.eventId() + " (" + event.orderId() + ")");
                }
            }
            double seconds = (System.nanoTime() - started) / 1e9;
            Log.info("publisher", String.format(java.util.Locale.ROOT,
                    "published %d messages in %.1f s (%.0f per second)", count, seconds, count / Math.max(seconds, 0.001)));
        }
    }
}
