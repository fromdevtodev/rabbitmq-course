package com.fromdevtodev.rabbitmq.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One service instance that wants every price change: it declares its <em>own</em> exclusive queue, lets the broker choose
 * the name, binds it to the fanout exchange and consumes from it.
 *
 * <p>The queue lives exactly as long as this connection. Start this command in several terminals and every instance
 * gets a copy of every message published while it is running. A message published while no instance is running reaches no
 * queue, and nobody keeps it for the instance that starts later. Acknowledgements are a later episode.
 */
final class Subscriber {
    private Subscriber() { }

    /** Runs until Ctrl+C, or until {@code limit} messages arrived ({@code limit <= 0}: no limit). */
    static void run(String name, long limit) throws IOException, TimeoutException, InterruptedException {
        AtomicLong received = new AtomicLong();
        CountDownLatch stopped = new CountDownLatch(1);
        try (Connection connection = Topology.connect("subscriber-" + name);
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            String queue = Topology.declareExclusiveQueue(channel);
            Topology.bind(channel, queue, "");
            channel.basicQos(1);
            DeliverCallback onMessage = (consumerTag, delivery) -> {
                PriceChange event = EventJson.fromBytes(delivery.getBody());
                Log.info(name, "received " + event.eventId() + " (routing key '"
                        + delivery.getEnvelope().getRoutingKey() + "')");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                if (limit > 0 && received.incrementAndGet() >= limit) {
                    stopped.countDown();
                }
            };
            channel.basicConsume(queue, false, onMessage, consumerTag -> stopped.countDown());
            Log.info(name, "subscribed with its own exclusive queue '" + queue + "' bound to '" + Rules.EXCHANGE
                    + "' (Ctrl+C to stop; the queue goes with the connection)");
            stopped.await();
        }
    }
}
