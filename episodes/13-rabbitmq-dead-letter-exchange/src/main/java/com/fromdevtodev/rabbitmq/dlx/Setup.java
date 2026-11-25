package com.fromdevtodev.rabbitmq.dlx;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Declares the topology in the two steps of the build: {@code dead-letter} (the exchange and the queue where failures go) and
 * {@code source} (the work queue that names that exchange). {@code all} runs both, in that order.
 */
final class Setup {
    private Setup() { }

    static void run(String stage, String deadLetterBindingKey, Topology.Options options) throws IOException, TimeoutException {
        if (!stage.equals("all") && !stage.equals("dead-letter") && !stage.equals("source")) {
            throw new IllegalArgumentException("unknown --stage " + stage + " (all, dead-letter or source)");
        }
        try (Connection connection = Topology.connect("setup");
             Channel channel = connection.createChannel()) {
            if (stage.equals("all") || stage.equals("dead-letter")) {
                Topology.declareDeadLetterSide(channel, deadLetterBindingKey);
                Log.info("setup", "dead-letter exchange '" + Topology.DEAD_LETTER_EXCHANGE + "' and queue '"
                        + Topology.DEAD_LETTER_QUEUE + "' declared, bound with key " + deadLetterBindingKey);
            }
            if (stage.equals("all") || stage.equals("source")) {
                Topology.declareSourceSide(channel, options);
                Log.info("setup", "queue '" + Topology.QUEUE + "' declared and bound to '" + Topology.EXCHANGE + "' with key "
                        + Topology.ROUTING_KEY + (options.deadLetter()
                        ? ", dead-letter exchange: " + Topology.DEAD_LETTER_EXCHANGE
                        : ", no dead-letter exchange"));
            }
        }
    }
}
