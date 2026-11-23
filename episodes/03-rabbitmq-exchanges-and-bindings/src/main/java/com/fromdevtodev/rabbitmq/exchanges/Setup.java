package com.fromdevtodev.rabbitmq.exchanges;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declares the starting point of the episode: the exchange, notifications.email and audit.log, and their bindings. */
final class Setup {
    private Setup() { }

    static void run() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareInitialQueues(channel);
            Log.info("setup", "exchange '" + Topology.EXCHANGE + "'; '" + Topology.EMAIL + "' bound with " + OrderEvent.CREATED
                    + "; '" + Topology.AUDIT + "' bound with " + OrderEvent.CANCELLED);
        }
    }
}
