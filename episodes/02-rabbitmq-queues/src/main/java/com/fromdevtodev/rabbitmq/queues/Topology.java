package com.fromdevtodev.rabbitmq.queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The only RabbitMQ object this episode is about: one durable queue, {@code notifications.email}.
 *
 * <p>Messages reach it through the default (nameless) exchange, which puts a message straight into the queue
 * whose name is the routing key. That is plumbing here; exchanges and bindings are episode 3.
 */
final class Topology {
    static final String QUEUE = "notifications.email";

    private Topology() { }

    /** Safe to call from every side: declaring a queue that already exists with the same settings does nothing. */
    static void declare(Channel channel) throws IOException {
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
        factory.setAutomaticRecoveryEnabled(true);
        return factory.newConnection(connectionName);
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
