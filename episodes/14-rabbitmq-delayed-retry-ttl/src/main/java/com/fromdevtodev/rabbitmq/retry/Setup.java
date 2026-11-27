package com.fromdevtodev.rabbitmq.retry;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** Declares the starting point (the work queue) and, as a separate step, the retry loop. */
final class Setup {
    private Setup() { }

    static void run() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            Log.info("setup", "queue '" + Topology.QUEUE + "' (classic) bound to '" + Topology.EXCHANGE + "' with key " + Topology.ROUTING_KEY);
        }
    }

    /** Adds the retry exchange, the retry queue with its message TTL, and the binding. The policy is set by run.sh. */
    static void buildRetry(long delayMillis) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("build-retry");
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            Topology.declareRetry(channel, delayMillis);
            Log.info("build-retry", "retry queue '" + Topology.RETRY_QUEUE + "': message TTL " + delayMillis + " ms, dead-letters to '"
                    + Topology.EXCHANGE + "', bound to '" + Topology.RETRY_EXCHANGE + "' with key " + Topology.ROUTING_KEY);
        }
    }
}
