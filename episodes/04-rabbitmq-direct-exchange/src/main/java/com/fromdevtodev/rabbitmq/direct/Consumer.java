package com.fromdevtodev.rabbitmq.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A consumer of one named queue: it prints each message it receives and then acknowledges it.
 *
 * <p>It declares its queue but <em>not</em> any binding: a queue that nobody has bound receives nothing, however
 * long you consume from it. Acknowledgements are a later episode; here they are plumbing.
 */
final class Consumer {
    private Consumer() { }

    /** Consumes from {@code queue} until Ctrl+C, or until {@code limit} messages arrived ({@code limit <= 0}: no limit). */
    static void run(String queue, long limit) throws IOException, TimeoutException, InterruptedException {
        AtomicLong received = new AtomicLong();
        CountDownLatch stopped = new CountDownLatch(1);
        try (Connection connection = Topology.connect("consumer-" + queue);
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel, queue);
            channel.basicQos(1);
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                LogEvent event = EventJson.fromBytes(delivery.getBody());
                Log.info("consumer", queue + " received " + event.eventId() + " with routing key '"
                        + delivery.getEnvelope().getRoutingKey() + "'");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                if (limit > 0 && received.incrementAndGet() >= limit) {
                    stopped.countDown();
                }
            };
            channel.basicConsume(queue, false, onMessage, consumerTag -> stopped.countDown());
            Log.info("consumer", "waiting for messages on '" + queue + "' (Ctrl+C to stop)");
            stopped.await();
        }
    }
}
