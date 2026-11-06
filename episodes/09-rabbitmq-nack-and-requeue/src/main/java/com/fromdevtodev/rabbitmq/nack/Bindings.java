package com.fromdevtodev.rabbitmq.nack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declaring the exchange, the queue and the binding. The producer is never involved. */
final class Bindings {
    private Bindings() { }

    static void setup() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            Topology.declareQueue(channel);
            Topology.bind(channel);
            Log.info("setup", "queue '" + Rules.QUEUE + "' (classic) is bound to exchange '" + Rules.EXCHANGE
                    + "' with binding key '" + Rules.BINDING_KEY + "'");
        }
    }

    /** Deletes the queue and the exchange so a demo starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("reset");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(Rules.QUEUE);
        }
        try (Connection connection = Topology.connect("reset-exchange")) {
            Channel channel = connection.createChannel();
            try {
                channel.exchangeDelete(Rules.EXCHANGE);
            } catch (IOException exchangeMissing) {
                // Nothing to delete; the demo can go on.
            } finally {
                if (channel.isOpen()) {
                    try {
                        channel.close();
                    } catch (IOException | TimeoutException ignored) {
                        // The work is done; a failed close changes nothing.
                    }
                }
            }
        }
        Log.info("reset", "exchange '" + Rules.EXCHANGE + "' and queue '" + Rules.QUEUE + "' deleted (if they existed)");
    }
}
