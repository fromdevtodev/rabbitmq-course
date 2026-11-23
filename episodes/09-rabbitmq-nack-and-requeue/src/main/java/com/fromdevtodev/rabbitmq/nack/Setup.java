package com.fromdevtodev.rabbitmq.nack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declares the starting point of both experiments: the exchange, the queue and the binding. */
final class Setup {
    private Setup() { }

    static void run() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            Log.info("setup", "queue '" + Topology.QUEUE + "' (classic) bound to '" + Topology.EXCHANGE + "' with key " + Topology.ROUTING_KEY);
        }
    }
}
