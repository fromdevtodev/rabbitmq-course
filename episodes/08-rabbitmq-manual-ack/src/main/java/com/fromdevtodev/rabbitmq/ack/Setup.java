package com.fromdevtodev.rabbitmq.ack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declares the queue and nothing else. */
final class Setup {
    private Setup() { }

    static void run() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel);
            Log.info("setup", "queue '" + Topology.QUEUE + "' declared");
        }
    }
}
