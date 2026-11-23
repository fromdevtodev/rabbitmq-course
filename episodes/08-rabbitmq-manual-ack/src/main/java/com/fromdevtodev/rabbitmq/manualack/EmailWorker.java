package com.fromdevtodev.rabbitmq.manualack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The email worker: it takes one message of {@code notifications.email} at a time and "sends" the email (nothing is really
 * sent: sending is a pause and a log line). Two choices make the demos:
 *
 * <ul>
 *   <li>{@link AckMode}: automatic or manual acknowledgement;</li>
 *   <li>{@link CrashPoint}: where, if anywhere, the worker dies.</li>
 * </ul>
 *
 * <p>{@code basicQos(1)} keeps one message in flight, so the counters are easy to read. Prefetch is the subject of episode 10.
 */
final class EmailWorker {
    private EmailWorker() { }

    /** Works until Ctrl+C, or until {@code limit} messages were handled ({@code limit <= 0}: no limit). */
    static void run(String name, AckMode mode, CrashPoint crash, long sendMillis, long limit)
            throws IOException, TimeoutException, InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicLong handled = new AtomicLong();  // one message at a time (basicQos 1), so no two callbacks overlap
        try (Connection connection = Topology.connect(name);
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel);
            // region readme:handle
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                OrderEvent event = EventJson.fromBytes(delivery.getBody());
                Log.info(name, "received " + event.eventId() + " (" + event.orderId() + "), redelivered="
                        + delivery.getEnvelope().isRedeliver());
                sendEmail(name, event, crash, sendMillis);
                if (crash == CrashPoint.AFTER_SEND) {
                    halt(name, "after the email was sent, before the ack");
                }
                if (mode == AckMode.MANUAL) {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    Log.info(name, "acknowledged " + event.eventId());
                }
                if (limit > 0 && handled.incrementAndGet() >= limit) {
                    stopped.countDown();
                }
            };
            // endregion
            // region readme:consume
            channel.basicQos(1);
            channel.basicConsume(Topology.QUEUE, mode.autoAck(), onMessage, consumerTag -> stopped.countDown());
            // endregion
            Log.info(name, "waiting for messages on '" + Topology.QUEUE + "' in " + mode.label() + " mode (Ctrl+C to stop)");
            stopped.await();
        }
    }

    /** The pretend email: a log line and a pause. With {@code WHILE_SENDING} the JVM halts halfway through the pause. */
    private static void sendEmail(String name, OrderEvent event, CrashPoint crash, long sendMillis) {
        Log.info(name, "sending the email for " + event.orderId() + " (takes " + sendMillis + " ms)");
        if (crash == CrashPoint.WHILE_SENDING) {
            sleep(sendMillis / 2);
            halt(name, "halfway through the email");
        }
        sleep(sendMillis);
        Log.info(name, "email sent for " + event.orderId());
    }

    // region readme:halt
    /** What a killed process looks like to the broker: no ack, no channel.close(), no connection.close(). */
    private static void halt(String name, String where) {
        Log.info(name, "crashing " + where + ": the JVM halts now, with no clean shutdown");
        System.out.flush();
        Runtime.getRuntime().halt(1);
    }
    // endregion

    private static void sleep(long millis) {
        try {
            Thread.sleep(Math.max(0, millis));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
