package com.fromdevtodev.rabbitmq.whyabroker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The plumbing this episode treats as "one box": one durable queue and the default (nameless) exchange.
 *
 * <p>Publishing to the default exchange with the queue's name as the routing key puts the message straight into
 * that queue. How queues, exchanges and bindings really work is the subject of the next episodes.
 */
final class Topology {
    static final String QUEUE = "notifications.email";

    private Topology() { }

    /** Safe to call from both sides: declaring a queue that already exists with the same settings does nothing. */
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
