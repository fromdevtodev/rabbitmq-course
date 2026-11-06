package com.fromdevtodev.rabbitmq.nack;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: the direct exchange {@code orders} and the queue {@code notifications.email}
 * bound to it with the key {@code order.created}.
 *
 * <p>The queue is declared as a <em>classic</em> queue on purpose, explicitly, so the demo does not depend on the
 * default queue type of your broker. Nothing here sets a dead-letter exchange: a message that is rejected with
 * {@code requeue = false} is simply discarded.
 */
final class Topology {
    private Topology() { }

    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(Rules.EXCHANGE, BuiltinExchangeType.DIRECT, true);
    }

    static void declareQueue(Channel channel) throws IOException {
        channel.queueDeclare(Rules.QUEUE, true, false, false, Map.of("x-queue-type", "classic"));
    }

    static void bind(Channel channel) throws IOException {
        channel.queueBind(Rules.QUEUE, Rules.EXCHANGE, Rules.BINDING_KEY);
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
