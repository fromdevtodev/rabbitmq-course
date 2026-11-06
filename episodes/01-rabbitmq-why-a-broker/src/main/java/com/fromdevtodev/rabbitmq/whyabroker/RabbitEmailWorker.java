package com.fromdevtodev.rabbitmq.whyabroker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

/**
 * The consumer side: takes one message at a time, sends the email, and acknowledges only after the email was
 * accepted. If this process dies before the acknowledgement, the broker delivers the message again; that is
 * the "duplicates" cost from the video, and you can trigger it yourself (see the README).
 */
public final class RabbitEmailWorker {
    private RabbitEmailWorker() { }

    public static void run(EmailClient email, Duration retryDelay) throws IOException, TimeoutException, InterruptedException {
        EmailJob job = new EmailJob(email, retryDelay);
        try (Connection connection = Topology.connect("email-worker");
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            channel.basicQos(1); // one unacknowledged message at a time: the worker sends one email at a time
            CountDownLatch stopped = new CountDownLatch(1);
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                long tag = delivery.getEnvelope().getDeliveryTag();
                OrderCreated event;
                try {
                    event = EventJson.fromBytes(delivery.getBody());
                } catch (IOException malformed) {
                    Log.info("email-worker", "malformed message, rejecting it without requeue: " + malformed.getMessage());
                    channel.basicNack(tag, false, false);
                    return;
                }
                Log.info("email-worker", "received " + event.eventId() + " for " + event.orderId());
                try {
                    job.handle(event);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stopped.countDown();
                    return; // not acknowledged: the broker will deliver it again
                }
                channel.basicAck(tag, false);
            };
            channel.basicConsume(Topology.QUEUE, false, onMessage, consumerTag -> stopped.countDown());
            Log.info("email-worker", "waiting for messages on '" + Topology.QUEUE + "' (Ctrl+C to stop)");
            stopped.await();
        }
    }
}
