package com.fromdevtodev.rabbitmq.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Everything about declaring queues and bindings. The producer is never involved. */
final class Bindings {
    private Bindings() { }

    /** Declares the exchange {@code logs}, its three queues and the five bindings of {@link Rules#BINDINGS}. */
    static void setup() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (String queue : Rules.QUEUES) {
                Topology.declareQueue(channel, queue);
            }
            for (Rules.Binding binding : Rules.BINDINGS) {
                Topology.bind(channel, binding.queue(), binding.key());
                Log.info("bind", "queue '" + binding.queue() + "' is bound to exchange '" + Rules.EXCHANGE
                        + "' with binding key '" + binding.key() + "'");
            }
        }
    }

    /**
     * Declares a queue and nothing else. RabbitMQ binds every queue to the default exchange by itself, using the queue's
     * own name as the binding key; no code of ours creates that binding.
     */
    static void declareQueue(String queue) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("declare-queue");
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel, queue);
            Log.info("declare-queue", "queue '" + queue + "' declared; no binding was requested");
        }
    }

    /**
     * Tries to bind a queue to the default exchange yourself. The broker refuses, and this prints what it answered.
     * A refused request closes the channel, which is why the attempt has a channel of its own.
     */
    static void tryBindToDefaultExchange(String queue, String key) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("try-bind-default")) {
            Channel channel = connection.createChannel();
            try {
                channel.queueBind(queue, "", key);
                Log.info("try-bind-default", "the broker accepted the binding (unexpected)");
            } catch (IOException refused) {
                Throwable reason = refused.getCause() != null ? refused.getCause() : refused;
                Log.info("try-bind-default", "the broker refused: " + reason.getMessage());
            } finally {
                if (channel.isOpen()) {
                    try {
                        channel.close();
                    } catch (IOException | TimeoutException ignored) {
                        // The attempt is over; a failed close changes nothing.
                    }
                }
            }
        }
    }

    /** Deletes the exchange and the four queues (and with them their bindings) so a demo starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("reset")) {
            try (Channel channel = connection.createChannel()) {
                for (String queue : Rules.QUEUES) {
                    channel.queueDelete(queue);
                }
                channel.queueDelete(Rules.EMAIL);
            }
            deleteExchangeIfPresent(connection);
            Log.info("reset", "exchange '" + Rules.EXCHANGE + "' and queues " + Rules.QUEUES + ", '" + Rules.EMAIL
                    + "' deleted (if they existed)");
        }
    }

    /** Uses its own channel: if the broker refuses (for example, the exchange does not exist) it closes that channel. */
    private static void deleteExchangeIfPresent(Connection connection) throws IOException {
        Channel channel = connection.createChannel();
        try {
            channel.exchangeDelete(Rules.EXCHANGE);
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
}
