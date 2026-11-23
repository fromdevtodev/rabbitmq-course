package com.fromdevtodev.rabbitmq.prefetch;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An email worker: it consumes {@code notifications.email}, "handles" each message for {@code delayMillis} (nothing is really
 * sent: handling is a pause and a log line) and acknowledges it afterwards.
 *
 * <p>The one setting this episode is about is {@code prefetch}, the number given to {@code basicQos} before the consumer starts.
 * The acknowledgement after the work belongs to episode 8 and is only here so that a message stays unacknowledged while the
 * worker handles it.
 */
final class Worker {
    private Worker() { }

    /** Consumes until Ctrl+C, or until {@code limit} messages were handled ({@code limit <= 0}: no limit). */
    static void run(String name, long delayMillis, int prefetch, long limit)
            throws IOException, TimeoutException, InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicLong received = new AtomicLong();  // this channel calls the callback one message at a time
        try (Connection connection = Topology.connect(name);
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel);
            // region readme:worker
            channel.basicQos(prefetch);   // at most this many unacknowledged messages for this consumer; 0 = no limit
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                OrderEvent event = EventJson.fromBytes(delivery.getBody());
                long position = received.incrementAndGet();
                Log.info(name, "received " + event.eventId() + " (" + event.orderId() + "), message " + position + " for this worker");
                sleep(delayMillis);
                Log.info(name, "handled " + event.eventId());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                if (limit > 0 && position >= limit) {
                    stopped.countDown();
                }
            };
            channel.basicConsume(Topology.QUEUE, false, onMessage, consumerTag -> stopped.countDown());
            // endregion
            Log.info(name, "waiting for messages on '" + Topology.QUEUE + "' (prefetch " + (prefetch == 0 ? "0 = no limit" : prefetch) + ", Ctrl+C to stop)");
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
