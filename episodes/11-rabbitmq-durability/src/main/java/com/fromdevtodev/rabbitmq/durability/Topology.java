package com.fromdevtodev.rabbitmq.durability;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * The two RabbitMQ objects of this episode: the queues {@code notifications.email} and {@code loyalty.points}, plus the
 * connection settings. Both queues are <em>durable</em>; what differs between the demos is how each <em>message</em> is published.
 *
 * <p>Messages go to the default exchange (the empty name) with the queue's name as routing key, as in episode 2.
 */
final class Topology {
    static final String EMAIL = "notifications.email";
    static final String POINTS = "loyalty.points";
    static final List<String> QUEUES = List.of(EMAIL, POINTS);

    private Topology() { }

    /** Safe to call from every side: declaring a queue that already exists with the same settings does nothing. */
    // region readme:declare
    static void declareQueue(Channel channel, String queue) throws IOException {
        // queue, durable, exclusive, autoDelete, arguments
        channel.queueDeclare(queue, true, false, false, null);
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

    /** Deletes both queues and everything in them, so a demo starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            for (String queue : QUEUES) {
                channel.queueDelete(queue);
                Log.info("reset", "queue '" + queue + "' deleted");
            }
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
