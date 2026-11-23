package com.fromdevtodev.rabbitmq.exchanges;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The loyalty-points service adds itself to the system: it declares its own queue and binds it to {@code orders}. Nothing
 * else changes: the publisher is the same code, and it is not restarted.
 *
 * <p>This class only sets the queue up and exits. Taking messages from {@code loyalty.points} is the same as in episode 2.
 */
final class PointsWorker {
    private PointsWorker() { }

    static void bind() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("points-worker");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            Topology.declareAndBind(channel, Topology.POINTS, OrderEvent.CREATED);
            Log.info("points-worker", "queue '" + Topology.POINTS + "' declared and bound to '" + Topology.EXCHANGE
                    + "' with " + OrderEvent.CREATED);
        }
    }
}
