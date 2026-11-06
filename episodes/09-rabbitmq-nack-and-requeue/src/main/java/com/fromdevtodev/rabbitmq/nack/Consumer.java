package com.fromdevtodev.rabbitmq.nack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The email worker. It takes <em>one message at a time</em> and answers every delivery: an ack when the email is
 * "sent", a nack when it is not. What the nack asks for ({@code requeue} true or false) depends on {@code policy}.
 *
 * <p>It declares its queue but binds nothing. Run {@code setup} first.
 */
final class Consumer {
    private Consumer() { }

    /**
     * Consumes until Ctrl+C, or until {@code limit} deliveries have been answered ({@code limit <= 0}: no limit).
     * Note that with the policy {@code requeue} a poison message is delivered again and again, so that limit counts
     * deliveries, not distinct messages.
     */
    static void run(Rules.OnFailure policy, long workMillis, int transientFailures, long limit)
            throws IOException, TimeoutException, InterruptedException {
        EmailWorker worker = new EmailWorker(workMillis, transientFailures);
        Attempts attempts = new Attempts();
        AtomicLong answered = new AtomicLong();
        CountDownLatch stopped = new CountDownLatch(1);
        try (Connection connection = Topology.connect("worker");
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel);
            channel.basicQos(1);
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                OrderEvent event = EventJson.fromBytes(delivery.getBody());
                int attempt = attempts.next(event.eventId());
                boolean redelivered = delivery.getEnvelope().isRedeliver();

                Exception failure = null;
                try {
                    worker.send(event, attempt);
                } catch (EmailWorker.InvalidOrderException | EmailWorker.TransientFailure e) {
                    failure = e;
                }

                Rules.Decision decision = Rules.decide(policy, failure);
                String detail = failure == null ? "email sent to " + event.customerEmail() : failure.getMessage();
                if (decision.ack()) {
                    channel.basicAck(deliveryTag, false);
                } else {
                    channel.basicNack(deliveryTag, false, decision.requeue());
                }
                Log.info("worker", Report.line(event.eventId(), attempt, redelivered, detail, decision));

                if (limit > 0 && answered.incrementAndGet() >= limit) {
                    stopped.countDown();
                }
            };
            channel.basicConsume(Rules.QUEUE, false, onMessage, consumerTag -> stopped.countDown());
            Log.info("worker", "waiting for messages on '" + Rules.QUEUE + "', on failure: " + policy
                    + " (Ctrl+C to stop)");
            stopped.await();
        }
    }
}
