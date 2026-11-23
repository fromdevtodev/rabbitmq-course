package com.fromdevtodev.rabbitmq.nack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The consumer, {@code email-worker}: it takes one message at a time, "sends" the confirmation email (a pause and a log line:
 * nothing is really sent) and answers RabbitMQ with an ack or a nack.
 *
 * <p>Two settings here belong to other episodes and are not explained in this one: the manual acknowledgement mode is episode 8, and
 * {@code basicQos(1)}, which makes the worker take one message at a time, is prefetch (episode 10). With it the worker has at most one
 * message in hand, so the queue's counters are easy to read.
 */
final class EmailWorker {
    private EmailWorker() { }

    /** Works until Ctrl+C, or until it has received {@code maxDeliveries} deliveries ({@code <= 0}: no limit). */
    static void run(String name, Rules.OnInvalid onInvalid, long workMillis, long maxDeliveries)
            throws IOException, TimeoutException, InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicLong deliveries = new AtomicLong();
        Attempts attempts = new Attempts();
        try (Connection connection = Topology.connect(name);
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            channel.basicQos(1);
            // region readme:worker
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                OrderEvent order = EventJson.fromBytes(delivery.getBody());
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                int attempt = attempts.next(order.eventId());
                Log.info(name, "received " + order.eventId() + " (" + order.orderId() + "), attempt " + attempt
                        + ", redelivered=" + delivery.getEnvelope().isRedeliver());
                sleep(workMillis);                                       // "sending the email"

                Rules.Failure failure = Rules.check(order);
                Rules.Outcome outcome = Rules.decide(failure, onInvalid);
                if (outcome == Rules.Outcome.ACK) {
                    channel.basicAck(deliveryTag, false);
                    Log.info(name, "sent the email for " + order.orderId() + " and acknowledged " + order.eventId());
                } else {
                    boolean requeue = Rules.requeue(outcome);
                    Log.info(name, failure.reason() + ": nack " + order.eventId() + ", requeue=" + requeue);
                    channel.basicNack(deliveryTag, false, requeue);
                }
                if (maxDeliveries > 0 && deliveries.incrementAndGet() >= maxDeliveries) {
                    stopped.countDown();
                }
            };
            channel.basicConsume(Topology.QUEUE, false, onMessage, consumerTag -> stopped.countDown());
            // endregion
            Log.info(name, "waiting for messages on '" + Topology.QUEUE + "', on invalid orders: "
                    + onInvalid.name().toLowerCase() + " (Ctrl+C to stop)");
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
