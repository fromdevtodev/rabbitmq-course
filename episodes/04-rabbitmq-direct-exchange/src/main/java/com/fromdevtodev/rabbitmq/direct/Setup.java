package com.fromdevtodev.rabbitmq.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declares the starting points of the two demos. */
final class Setup {
    private Setup() { }

    /** Demo 1: the exchange {@code logs}, errors-pager, audit and metrics, and their five bindings. */
    static void routing() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareAll(channel);
            Log.info("setup", "exchange '" + Topology.EXCHANGE + "' with " + Topology.BINDINGS.size() + " bindings: "
                    + describe());
        }
    }

    /** Demo 2: one queue and no binding call at all. Whatever binding it has, the broker made it. */
    static void queueOnly(String queue) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(queue, true, false, false, null);
            Log.info("setup", "queue '" + queue + "' declared; no binding was created by us");
        }
    }

    private static String describe() {
        StringBuilder text = new StringBuilder();
        for (Binding binding : Topology.BINDINGS) {
            text.append(text.isEmpty() ? "" : ", ").append(binding.queue()).append(" <- ").append(binding.key());
        }
        return text.toString();
    }
}
