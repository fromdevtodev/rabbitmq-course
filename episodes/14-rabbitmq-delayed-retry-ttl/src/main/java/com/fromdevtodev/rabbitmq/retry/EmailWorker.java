package com.fromdevtodev.rabbitmq.retry;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The consumer, {@code email-worker}: one message at a time, "sends" the email through the fake {@link MailServer} (nothing is really
 * sent) and answers RabbitMQ. Manual acknowledgement is episode 8 and {@code basicQos(1)} is prefetch (episode 10): both only make the
 * counters easy to read here.
 */
final class EmailWorker {
    private EmailWorker() { }

    static void run(String name, Rules.OnFailure onFailure, long workMillis, long sleepMillis, Set<String> affectedEvents,
                    long downMillis, long maxDeliveries)
            throws IOException, TimeoutException, InterruptedException {
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicLong deliveries = new AtomicLong();
        Attempts attempts = new Attempts();
        MailServer mailServer = new MailServer(affectedEvents, downMillis, System::currentTimeMillis);
        try (Connection connection = Topology.connect(name);
             Channel channel = connection.createChannel()) {
            if (onFailure == Rules.OnFailure.RETRY_QUEUE && !Topology.retryQueueExists(connection)) {
                throw new IllegalStateException("The retry queue does not exist. Run './run.sh build' first.");
            }
            Topology.declare(channel);
            channel.basicQos(1);
            // region readme:worker
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                OrderEvent order = EventJson.fromBytes(delivery.getBody());
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                int attempt = attempts.next(order.eventId());
                long rejected = Deaths.rejections(delivery.getProperties().getHeaders(), Topology.QUEUE);
                Log.info(name, "received " + order.eventId() + " (" + order.orderId() + "), attempt " + attempt
                        + (rejected > 0 ? ", rejected " + rejected + " time(s) before (x-death)" : ""));
                sleep(workMillis);                                       // "calling the mail server"

                Rules.Failure failure = mailServer.send(order);
                Rules.Outcome outcome = Rules.decide(failure, onFailure);
                if (outcome == Rules.Outcome.ACK) {
                    channel.basicAck(deliveryTag, false);
                    Log.info(name, "sent the email for " + order.orderId() + " and acknowledged " + order.eventId());
                } else {
                    Log.info(name, failure.reason() + " (call " + mailServer.callsFor(order.eventId()) + " for " + order.eventId() + ")");
                    if (outcome == Rules.Outcome.SLEEP_THEN_REQUEUE) {
                        Log.info(name, "sleeping " + sleepMillis + " ms before trying again, holding " + order.eventId());
                        sleep(sleepMillis);                              // the anti-pattern: the worker waits
                    }
                    boolean requeue = Rules.requeue(outcome);
                    Log.info(name, "nack " + order.eventId() + ", requeue=" + requeue);
                    channel.basicNack(deliveryTag, false, requeue);
                }
                if (maxDeliveries > 0 && deliveries.incrementAndGet() >= maxDeliveries) {
                    stopped.countDown();
                }
            };
            channel.basicConsume(Topology.QUEUE, false, onMessage, consumerTag -> stopped.countDown());
            // endregion
            Log.info(name, "waiting for messages on '" + Topology.QUEUE + "', on failure: " + onFailure.name().toLowerCase().replace('_', '-')
                    + " (Ctrl+C to stop)");
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
