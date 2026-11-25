package com.fromdevtodev.rabbitmq.confirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declares the queue {@code orders.in}, with or without a length limit, and nothing else. */
final class Setup {
    private Setup() { }

    static void run(Integer maxLength) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel, maxLength);
            Log.info("setup", "queue '" + Topology.ORDERS_IN + "' declared: durable, classic, "
                    + (maxLength == null ? "no length limit" : "at most " + maxLength + " messages, overflow reject-publish"));
        }
    }
}
