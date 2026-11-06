package com.fromdevtodev.rabbitmq.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/** Everything about declaring queues and bindings. The producer is never involved. */
final class Bindings {
    private Bindings() { }

    /** Declares the exchange {@code events}, the queues of {@code bindings} and one binding for each. */
    static void setup(List<Rules.Binding> bindings) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (Rules.Binding binding : bindings) {
                Topology.declareQueue(channel, binding.queue());
                Topology.bind(channel, binding.queue(), binding.pattern());
                log(binding.queue(), binding.pattern());
            }
        }
    }

    /** One more binding, made by whoever owns the queue. The producer does not change. */
    static void bind(String queue, String pattern) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("bind");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            Topology.declareQueue(channel, queue);
            Topology.bind(channel, queue, pattern);
            log(queue, pattern);
        }
    }

    private static void log(String queue, String pattern) {
        Log.info("bind", "queue '" + queue + "' is bound to exchange '" + Rules.EXCHANGE + "' with pattern '" + pattern + "'");
    }

    /**
     * Deletes the exchange and every queue of the project, plus the extra queues you created with {@code bind}, so a demo
     * starts from nothing. Deleting a queue deletes its bindings too.
     */
    static void reset(List<String> extraQueues) throws IOException, TimeoutException {
        List<String> queues = new java.util.ArrayList<>(Rules.allQueues());
        queues.addAll(extraQueues);
        try (Connection connection = Topology.connect("reset")) {
            try (Channel channel = connection.createChannel()) {
                for (String queue : queues) {
                    channel.queueDelete(queue);
                }
            }
            deleteExchangeIfPresent(connection);
            Log.info("reset", "exchange '" + Rules.EXCHANGE + "' and queues " + queues + " deleted (if they existed)");
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
