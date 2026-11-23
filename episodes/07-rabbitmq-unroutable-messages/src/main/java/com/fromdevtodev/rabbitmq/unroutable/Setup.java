package com.fromdevtodev.rabbitmq.unroutable;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declares the starting point of each experiment. */
final class Setup {
    private Setup() { }

    /** Experiments 1 and 2: the exchange {@code orders}, three queues and five bindings. */
    static void orders() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareOrders(channel);
            Log.info("setup", "exchange '" + Topology.EXCHANGE + "' with " + Topology.ORDERS.size() + " bindings: " + describe());
        }
    }

    /** Experiment 3: the same, plus the exchange {@code orders.unrouted} and the queue {@code unrouted.parking}. Nothing is attached yet. */
    static void ordersWithParking() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareOrders(channel);
            Topology.declareParking(channel);
            Log.info("setup", "exchange '" + Topology.EXCHANGE + "' with " + Topology.ORDERS.size() + " bindings: " + describe());
            Log.info("setup", "exchange '" + Topology.UNROUTED_EXCHANGE + "' (fanout) with queue '" + Topology.PARKING
                    + "'; not attached to '" + Topology.EXCHANGE + "' yet");
        }
    }

    private static String describe() {
        StringBuilder text = new StringBuilder();
        for (Binding binding : Topology.ORDERS) {
            text.append(text.isEmpty() ? "" : ", ").append(binding.queue()).append(" <- ").append(binding.key());
        }
        return text.toString();
    }
}
