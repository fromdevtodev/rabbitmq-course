package com.fromdevtodev.rabbitmq.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declares the starting point of each experiment. */
final class Setup {
    private Setup() { }

    /** Experiment 1: the exchange {@code price-updates} and three queues, bound with three different keys. */
    static void broadcast() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareAll(channel, Topology.BROADCAST);
            Log.info("setup", "exchange '" + Topology.EXCHANGE + "' with " + Topology.BROADCAST.size() + " bindings: " + describe());
        }
    }

    /** Experiment 2, design one: the exchange and one queue, {@code cache.shared}, for consumers that share it. */
    static void shared() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            Topology.declareAndBind(channel, Topology.SHARED, "");
            Log.info("setup", "exchange '" + Topology.EXCHANGE + "' with one queue: " + Topology.SHARED);
        }
    }

    /** Only the exchange, with no queue bound: what a fanout exchange does when there is nobody to copy to. */
    static void empty() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            Log.info("setup", "exchange '" + Topology.EXCHANGE + "' with no queue bound");
        }
    }

    private static String describe() {
        StringBuilder text = new StringBuilder();
        for (Binding binding : Topology.BROADCAST) {
            text.append(text.isEmpty() ? "" : ", ").append(binding.queue()).append(" <- '").append(binding.key()).append("'");
        }
        return text.toString();
    }
}
