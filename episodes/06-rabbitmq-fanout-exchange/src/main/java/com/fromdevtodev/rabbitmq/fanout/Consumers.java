package com.fromdevtodev.rabbitmq.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

/**
 * The receivers of experiment 2. Both commands run until you stop them (Ctrl+C) and print one line per message:
 * {@code <name> received evt_0001 ...}. Messages are acknowledged automatically; manual acknowledgements are a later episode.
 */
final class Consumers {
    private Consumers() { }

    /** A consumer of a queue that other consumers may share: {@code cache-a} and {@code cache-b} of design one. */
    static void onSharedQueue(String name, String queue) throws IOException, TimeoutException, InterruptedException {
        Connection connection = Topology.connect(name);
        closeOnExit(connection);
        Channel channel = connection.createChannel();
        Log.info(name, "ready on queue " + queue);
        listen(name, channel, queue);
        new CountDownLatch(1).await();
    }

    // region readme:instance
    /** One instance of the catalog service: its own exclusive queue, bound to the exchange, and a consumer for it. */
    static void instance(String name) throws IOException, TimeoutException, InterruptedException {
        Connection connection = Topology.connect(name);
        closeOnExit(connection);
        Channel channel = connection.createChannel();
        Topology.declareExchange(channel);
        String queue = Topology.declareExclusiveAndBind(channel);
        Log.info(name, "ready on queue " + queue);
        listen(name, channel, queue);
        new CountDownLatch(1).await();
    }
    // endregion

    // region readme:listen
    private static void listen(String name, Channel channel, String queue) throws IOException {
        DeliverCallback onMessage = (consumerTag, delivery) -> {
            PriceUpdate update = EventJson.fromBytes(delivery.getBody());
            Log.info(name, "received " + update.eventId() + " (" + update.summary() + ")");
        };
        channel.basicConsume(queue, true, onMessage, consumerTag -> { });
    }
    // endregion

    private static void closeOnExit(Connection connection) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                connection.close();
            } catch (IOException | RuntimeException ignored) {
                // the process is ending anyway; the broker notices a closed socket as well
            }
        }));
    }
}
