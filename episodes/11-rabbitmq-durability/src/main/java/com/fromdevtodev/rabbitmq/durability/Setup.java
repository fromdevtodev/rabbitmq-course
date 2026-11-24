package com.fromdevtodev.rabbitmq.durability;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declares the two durable queues and nothing else. */
final class Setup {
    private Setup() { }

    static void run() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            for (String queue : Topology.QUEUES) {
                Topology.declareQueue(channel, queue);
                Log.info("setup", "queue '" + queue + "' declared (durable)");
            }
        }
    }
}
