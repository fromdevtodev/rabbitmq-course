package com.fromdevtodev.rabbitmq.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Everything about declaring queues and bindings. The producer is never involved. */
final class Bindings {
    private Bindings() { }

    /** Declares the exchange {@code price-updates}, its three queues and the three bindings of {@link Rules#BINDINGS}. */
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

    /** Declares the one queue that several consumers will share, and binds it to the exchange. */
    static void setupShared() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup-shared");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            Topology.declareQueue(channel, Rules.SHARED);
            Topology.bind(channel, Rules.SHARED, "");
            Log.info("bind", "queue '" + Rules.SHARED + "' is bound to exchange '" + Rules.EXCHANGE
                    + "' with binding key ''");
        }
    }

    /** Deletes the exchange and the queues (and with them their bindings) so a demo starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("reset")) {
            try (Channel channel = connection.createChannel()) {
                for (String queue : Rules.QUEUES) {
                    channel.queueDelete(queue);
                }
                channel.queueDelete(Rules.SHARED);
            }
            deleteExchangeIfPresent(connection);
            Log.info("reset", "exchange '" + Rules.EXCHANGE + "' and queues " + Rules.QUEUES + ", '" + Rules.SHARED
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
