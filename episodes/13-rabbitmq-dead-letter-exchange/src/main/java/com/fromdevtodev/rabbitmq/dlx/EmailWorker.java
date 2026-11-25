package com.fromdevtodev.rabbitmq.dlx;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The consumer, {@code email-worker}: one message at a time, manual acknowledgement, "sends" the email (a pause and a log line:
 * nothing is really sent). Manual acks are episode 8, nack and requeue episode 9, {@code basicQos(1)} episode 10: none of them is
 * explained here. The worker does not declare anything: run {@code setup} first.
 */
final class EmailWorker {
    private EmailWorker() { }

    /**
     * Works until Ctrl+C, or until it has received {@code maxDeliveries} deliveries ({@code <= 0}: no limit).
     *
     * <p>{@code releaseFile} is a recording aid, empty for everything else: when it is set, the worker handles delivery number N
     * only after that file has N lines, so that a script can make each event happen while the narrator talks about it.
     */
    static void run(String name, long workMillis, long maxDeliveries, String releaseFile) throws IOException, TimeoutException, InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicLong deliveries = new AtomicLong();
        AtomicLong arrived = new AtomicLong();
        try (Connection connection = Topology.connect(name);
             Channel channel = connection.createChannel()) {
            channel.basicQos(1);
            // region readme:worker
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                OrderEvent order = EventJson.fromBytes(delivery.getBody());
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                Log.info(name, "received " + order.eventId() + " (" + order.orderId() + ")");
                sleep(workMillis);                                       // "sending the email"

                Rules.Failure failure = Rules.check(order);
                Rules.Outcome outcome = Rules.decide(failure);
                if (outcome == Rules.Outcome.ACK) {
                    channel.basicAck(deliveryTag, false);
                    Log.info(name, "sent the email for " + order.orderId() + " and acknowledged " + order.eventId());
                } else {
                    Log.info(name, failure.reason() + ": nack " + order.eventId() + ", requeue=false");
                    channel.basicNack(deliveryTag, false, false);
                }
                if (maxDeliveries > 0 && deliveries.incrementAndGet() >= maxDeliveries) {
                    stopped.countDown();
                }
            };
            // endregion
            // Recording aid: with a release file, delivery N is handled only when the file has N lines. Without one, no wait.
            DeliverCallback gated = (consumerTag, delivery) -> {
                awaitRelease(releaseFile, arrived.incrementAndGet());
                onMessage.handle(consumerTag, delivery);
            };
            channel.basicConsume(Topology.QUEUE, false, gated, consumerTag -> stopped.countDown());   // false: manual acknowledgements
            Log.info(name, "waiting for messages on '" + Topology.QUEUE + "' (Ctrl+C to stop)");
            stopped.await();
        }
    }

    /** Returns when {@code releaseFile} has at least {@code number} lines (at once when no file is given). */
    private static void awaitRelease(String releaseFile, long number) {
        if (releaseFile == null || releaseFile.isBlank()) {
            return;
        }
        Path file = Path.of(releaseFile);
        try {
            while (!Files.exists(file) || Files.readAllLines(file).size() < number) {
                Thread.sleep(100);
            }
        } catch (IOException ignored) {
            // the file could not be read: carry on rather than block the worker
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
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
