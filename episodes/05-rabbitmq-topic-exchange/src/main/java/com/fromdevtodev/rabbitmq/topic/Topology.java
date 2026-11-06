package com.fromdevtodev.rabbitmq.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: the exchange {@code events}, its queues, and the bindings between them. The
 * names and the bindings themselves are in {@link Rules}.
 *
 * <p>The exchange is declared as a <em>topic</em> exchange: a binding matches when its pattern matches the words of the
 * routing key. A direct exchange (episode 4) would compare the two keys for equality instead.
 */
final class Topology {
    private Topology() { }

    /** Safe to call from every side: declaring an exchange that already exists with the same settings does nothing. */
    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(Rules.EXCHANGE, BuiltinExchangeType.TOPIC, true);
    }

    /** Declaring a queue that already exists with the same settings does nothing. */
    static void declareQueue(Channel channel, String queue) throws IOException {
        channel.queueDeclare(queue, true, false, false, null);
    }

    /** The pattern goes where a direct exchange takes a plain binding key. */
    static void bind(Channel channel, String queue, String pattern) throws IOException {
        channel.queueBind(queue, Rules.EXCHANGE, pattern);
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
