package com.fromdevtodev.rabbitmq.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** The one thing you cannot do with the default exchange: add a binding of your own to it. */
final class DefaultExchange {
    private DefaultExchange() { }

    // region readme:bind-default
    /** Tries to bind a queue to the default exchange (the empty name). The broker refuses, and closes the channel. */
    static void tryToBind(String queue, String key) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("log-service");
             Channel channel = connection.createChannel()) {
            channel.queueBind(queue, "", key);
            Log.info("log-service", "binding to the default exchange was accepted");
        } catch (IOException refused) {
            Throwable reason = refused.getCause() == null ? refused : refused.getCause();
            Log.info("log-service", "bind to the default exchange was refused: " + reason.getMessage());
        }
    }
    // endregion
}
