package com.fromdevtodev.rabbitmq.nack;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: the direct exchange {@code orders}, the queue {@code notifications.email} and one binding
 * with the key {@code order.created}. There is NO dead-letter exchange and no policy: a message that the worker rejects without
 * requeue is discarded. The queue is declared as a classic queue explicitly, so the demo does not depend on the default queue type.
 */
final class Topology {
    static final String EXCHANGE = "orders";
    static final String QUEUE = "notifications.email";
    static final String ROUTING_KEY = "order.created";

    private Topology() { }

    // region readme:topology
    static void declare(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(QUEUE, true, false, false, Map.of("x-queue-type", "classic"));
        channel.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);
    }
    // endregion

    static Connection connect(String connectionName) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(setting("RABBITMQ_HOST", "localhost"));
        factory.setPort(Integer.parseInt(setting("RABBITMQ_PORT", "5672")));
        factory.setUsername(setting("RABBITMQ_USERNAME", "guest"));
        factory.setPassword(setting("RABBITMQ_PASSWORD", "guest"));
        factory.setVirtualHost(setting("RABBITMQ_VHOST", "/"));
        factory.setConnectionTimeout(5_000);
        // The connection name shows up in the management UI and in `rabbitmqctl list_connections name`.
        return factory.newConnection(connectionName);
    }

    /** Deletes the queue and the exchange (and with them the binding and every message), so an experiment starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(QUEUE);
            channel.exchangeDelete(EXCHANGE);
            Log.info("reset", "queue '" + QUEUE + "' and exchange '" + EXCHANGE + "' deleted");
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
