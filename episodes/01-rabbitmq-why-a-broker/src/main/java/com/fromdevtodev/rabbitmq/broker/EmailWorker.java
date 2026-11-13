package com.fromdevtodev.rabbitmq.broker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The consumer: it takes one {@code order.created} message at a time from {@code notifications.email} and "sends" the
 * confirmation email through the {@link EmailProvider}.
 *
 * <p>Two settings here belong to later episodes and are not explained in this one: the worker acknowledges each message after
 * the email was sent (episode 8), and it takes one message at a time (prefetch, episode 10). They are what makes the queue's
 * numbers easy to read.
 */
final class EmailWorker {
    private EmailWorker() { }

    /** Consumes until Ctrl+C, or until {@code limit} messages were processed ({@code limit <= 0}: no limit). */
    static void run(String name, EmailProvider provider, long limit)
            throws IOException, TimeoutException, InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicLong processed = new AtomicLong();
        try (Connection connection = Topology.connect(name);
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel);
            // region readme:worker
            channel.basicQos(1);
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                OrderEvent event = EventJson.fromBytes(delivery.getBody());
                Log.info(name, "received " + event.eventId() + " (" + event.orderId() + "), sending the email ...");
                try {
                    provider.send(event.orderId());
                } catch (ProviderUnavailableException unavailable) {
                    // Retrying and dead-lettering are later episodes; here the demos start the worker only when the provider is up.
                    Log.info(name, "provider unavailable, stopping: " + unavailable.getMessage());
                    stopped.countDown();
                    return;
                }
                Log.info(name, "email sent for " + event.orderId());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                if (limit > 0 && processed.incrementAndGet() >= limit) {
                    stopped.countDown();
                }
            };
            channel.basicConsume(Topology.QUEUE, false, onMessage, consumerTag -> stopped.countDown());
            // endregion
            Log.info(name, "waiting for messages on '" + Topology.QUEUE + "', email provider " + provider.describe()
                    + " (Ctrl+C to stop)");
            stopped.await();
        }
    }
}
