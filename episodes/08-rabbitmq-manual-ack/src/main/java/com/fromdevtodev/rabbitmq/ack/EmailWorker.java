package com.fromdevtodev.rabbitmq.ack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A consumer of {@code notifications.email} that "sends" a confirmation email for each order event.
 *
 * <p>Nothing is really sent: sending is a sleep and a log line, so that the moments that matter are visible. What the
 * worker does with the acknowledgement depends on two settings:
 *
 * <ul>
 *   <li>{@link AckMode}: {@code AUTOMATIC} asks RabbitMQ not to wait for any answer; {@code MANUAL} calls {@code basicAck}
 *       only <em>after</em> the email was "sent".</li>
 *   <li>{@link CrashPoint}: where the worker stops abruptly, to show what RabbitMQ does with the message.</li>
 * </ul>
 */
final class EmailWorker {
    private EmailWorker() { }

    /**
     * Consumes until Ctrl+C, or until {@code limit} messages were processed ({@code limit <= 0}: no limit).
     *
     * @param sendMillis how long "sending the email" takes; a crash {@code WHILE_SENDING} happens halfway through
     */
    static void run(String name, AckMode mode, CrashPoint crash, long sendMillis, long limit)
            throws IOException, TimeoutException, InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicLong processed = new AtomicLong();
        try (Connection connection = Topology.connect(name);
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel);
            if (mode == AckMode.MANUAL) {
                // One unacknowledged message at a time, so the demo shows one message in flight.
                // How many deliveries may be in flight is the subject of the prefetch episode.
                channel.basicQos(1);
            }
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                OrderEvent event = EventJson.fromBytes(delivery.getBody());
                // The redelivered flag is a hint that the message was delivered before. Never use it to decide
                // whether work is safe to do: its absence proves nothing.
                Log.info(name, "received " + event.eventId() + " (" + event.orderId() + "), redelivered="
                        + delivery.getEnvelope().isRedeliver());

                Log.info(name, "sending the confirmation email for " + event.orderId() + " ...");
                sleep(sendMillis / 2);
                if (crash == CrashPoint.WHILE_SENDING) {
                    crashNow(name, "halfway through the send");
                }
                sleep(sendMillis - sendMillis / 2);
                Log.info(name, "email sent for " + event.orderId());
                if (crash == CrashPoint.AFTER_SEND) {
                    crashNow(name, "after the send, before the ack");
                }

                if (mode == AckMode.MANUAL) {
                    // The ack goes AFTER the work succeeded. Not before it, and not in a finally block.
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    Log.info(name, "acked " + event.eventId());
                }
                if (limit > 0 && processed.incrementAndGet() >= limit) {
                    stopped.countDown();
                }
            };
            channel.basicConsume(Topology.QUEUE, mode.autoAck(), onMessage, consumerTag -> stopped.countDown());
            Log.info(name, "waiting for messages on '" + Topology.QUEUE + "' in " + mode + " acknowledgement mode"
                    + (crash == CrashPoint.NONE ? "" : " (will crash " + crash + ")") + " (Ctrl+C to stop)");
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

    /**
     * Stops the JVM without running shutdown hooks and without closing the channel or the connection. The operating system
     * closes the sockets, which is all the broker ever sees of a killed process.
     */
    private static void crashNow(String name, String where) {
        Log.info(name, "CRASH (" + where + "): halting the JVM now, no clean shutdown");
        System.out.flush();
        Runtime.getRuntime().halt(137);
    }
}
