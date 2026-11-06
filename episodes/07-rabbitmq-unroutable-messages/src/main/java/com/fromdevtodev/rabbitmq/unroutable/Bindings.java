package com.fromdevtodev.rabbitmq.unroutable;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/** Declaring exchanges, queues and bindings. The producer is never involved. */
final class Bindings {
    private Bindings() { }

    /** Declares the exchange {@code orders}, its three queues and the five bindings of {@link Rules#BINDINGS}. */
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
     * Declares the alternate exchange {@code orders.unrouted} (fanout) and the queue {@code unrouted.parking} behind it.
     * This does <em>not</em> attach it to {@code orders}; that is a policy ({@code rabbitmqctl set_policy}).
     */
    static void setupParking() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup-parking");
             Channel channel = connection.createChannel()) {
            Topology.declareAlternate(channel);
            Log.info("setup-parking", "exchange '" + Rules.ALTERNATE_EXCHANGE + "' (fanout) and queue '"
                    + Rules.PARKING_QUEUE + "' are ready; they are not attached to '" + Rules.EXCHANGE + "' yet");
        }
    }

    /** Deletes both exchanges and all four queues (and with them their bindings) so a demo starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        List<String> queues = List.of(Rules.EMAIL, Rules.POINTS, Rules.AUDIT, Rules.PARKING_QUEUE);
        try (Connection connection = Topology.connect("reset")) {
            try (Channel channel = connection.createChannel()) {
                for (String queue : queues) {
                    channel.queueDelete(queue);
                }
            }
            deleteExchangeIfPresent(connection, Rules.EXCHANGE);
            deleteExchangeIfPresent(connection, Rules.ALTERNATE_EXCHANGE);
            Log.info("reset", "exchanges '" + Rules.EXCHANGE + "', '" + Rules.ALTERNATE_EXCHANGE + "' and queues "
                    + queues + " deleted (if they existed). A policy is not deleted: see the README.");
        }
    }

    /** Uses its own channel: if the broker refuses (for example, the exchange does not exist) it closes that channel. */
    private static void deleteExchangeIfPresent(Connection connection, String exchange) throws IOException {
        Channel channel = connection.createChannel();
        try {
            channel.exchangeDelete(exchange);
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
