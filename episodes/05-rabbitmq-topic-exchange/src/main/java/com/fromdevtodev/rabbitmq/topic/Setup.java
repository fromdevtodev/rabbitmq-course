package com.fromdevtodev.rabbitmq.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/** Declares the starting point of each experiment. */
final class Setup {
    private Setup() { }

    /** Experiment 1: the exchange {@code events}, four queues and four patterns. */
    static void routing() throws IOException, TimeoutException {
        declare(Topology.ROUTING);
    }

    /** Experiment 2: the exchange {@code events}, and the queues that tell star, hash and a star inside a word apart. */
    static void comparison() throws IOException, TimeoutException {
        declare(Topology.COMPARISON);
    }

    private static void declare(List<Binding> bindings) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareAll(channel, bindings);
            Log.info("setup", "exchange '" + Topology.EXCHANGE + "' with " + bindings.size() + " bindings: " + describe(bindings));
        }
    }

    private static String describe(List<Binding> bindings) {
        StringBuilder text = new StringBuilder();
        for (Binding binding : bindings) {
            text.append(text.isEmpty() ? "" : ", ").append(binding.queue()).append(" <- ").append(binding.pattern());
        }
        return text.toString();
    }
}
