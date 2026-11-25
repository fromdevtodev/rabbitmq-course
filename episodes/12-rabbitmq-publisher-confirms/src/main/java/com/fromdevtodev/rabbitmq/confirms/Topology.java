package com.fromdevtodev.rabbitmq.confirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * The one RabbitMQ object of this episode: the queue {@code orders.in}, plus the connection settings. The queue is durable and
 * classic. Depending on the experiment it has a length limit of two messages with {@code reject-publish}, which makes a full queue
 * answer a publish with a nack, or no limit at all.
 *
 * <p>Messages go to the default exchange (the empty name) with the queue's name as routing key, as in episode 2.
 */
final class Topology {
    static final String ORDERS_IN = "orders.in";

    private Topology() { }

    // region readme:declare
    /** {@code maxLength == null}: no limit. Otherwise a full queue rejects the next publish, and the broker answers it with a nack. */
    static void declareQueue(Channel channel, Integer maxLength) throws IOException {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-queue-type", "classic");
        if (maxLength != null) {
            arguments.put("x-max-length", maxLength);
            arguments.put("x-overflow", "reject-publish");
        }
        // queue, durable, exclusive, autoDelete, arguments
        channel.queueDeclare(ORDERS_IN, true, false, false, arguments);
    }
    // endregion

    /**
     * The publisher does not declare the queue: it only checks that it exists. Declaring it again with other arguments would be
     * refused by the broker, so the queue is declared in one place, {@link Setup}.
     */
    static void requireQueue(Channel channel) throws IOException {
        channel.queueDeclarePassive(ORDERS_IN);
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
            channel.queueDelete(ORDERS_IN);
            Log.info("reset", "queue '" + ORDERS_IN + "' deleted");
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
