package com.fromdevtodev.rabbitmq.exchanges;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Everything about bindings. This is what a consumer does when it starts: declare its own queue and bind it to the
 * exchange. The producer is not touched, restarted or redeployed.
 */
final class Bindings {
    private Bindings() { }

    /** Declares the exchange, the queues of the starting bindings, and the starting bindings themselves. */
    static void setup() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            for (Topology.Binding binding : Topology.INITIAL_BINDINGS) {
                Topology.bind(channel, binding.queue(), binding.routingKey());
                logBound(binding.queue(), binding.routingKey());
            }
        }
    }

    /** Binds {@code queue} to the exchange with {@code routingKey}, creating the queue if needed. */
    static void bind(String queue, String routingKey) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("bind");
             Channel channel = connection.createChannel()) {
            Topology.bind(channel, queue, routingKey);
            logBound(queue, routingKey);
        }
    }

    /** Removes one binding. The queue and the messages already in it stay. */
    static void unbind(String queue, String routingKey) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("unbind");
             Channel channel = connection.createChannel()) {
            channel.queueUnbind(queue, Topology.EXCHANGE, routingKey);
            Log.info("unbind", "queue '" + queue + "' is no longer bound to exchange '" + Topology.EXCHANGE
                    + "' with routing key '" + routingKey + "'");
        }
    }

    /** Deletes the queues and the exchange (and with them their bindings) so a demo starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("reset")) {
            try (Channel channel = connection.createChannel()) {
                for (String queue : Topology.QUEUES) {
                    channel.queueDelete(queue);
                }
            }
            deleteExchangeIfPresent(connection);
            Log.info("reset", "exchange '" + Topology.EXCHANGE + "' and queues " + Topology.QUEUES
                    + " deleted (if they existed)");
        }
    }

    /** Uses its own channel: if the broker refuses (for example, the exchange does not exist) it closes that channel. */
    private static void deleteExchangeIfPresent(Connection connection) throws IOException {
        Channel channel = connection.createChannel();
        try {
            channel.exchangeDelete(Topology.EXCHANGE);
        } catch (IOException exchangeMissingOrInUse) {
            // Nothing to delete, or it cannot be deleted right now; either way the demo can go on.
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

    private static void logBound(String queue, String routingKey) {
        Log.info("bind", "queue '" + queue + "' is bound to exchange '" + Topology.EXCHANGE
                + "' with routing key '" + routingKey + "'");
    }
}
