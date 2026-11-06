package com.fromdevtodev.rabbitmq.ack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The one RabbitMQ object of this episode: the queue {@code notifications.email}, plus the connection settings.
 *
 * <p>There is no exchange here. Messages are published to the default exchange (the empty name) with the queue's name as
 * routing key, as in episode 2; routing is not the subject of this episode.
 */
final class Topology {
    static final String QUEUE = "notifications.email";

    private Topology() { }

    /** Safe to call from every side: declaring a queue that already exists with the same settings does nothing. */
    static void declareQueue(Channel channel) throws IOException {
        channel.queueDeclare(QUEUE, true, false, false, null);
    }

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

    /** Deletes the queue and everything in it, so a demo starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(QUEUE);
            Log.info("reset", "queue '" + QUEUE + "' deleted");
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
