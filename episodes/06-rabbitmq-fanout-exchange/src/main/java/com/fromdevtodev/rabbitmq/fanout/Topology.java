package com.fromdevtodev.rabbitmq.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: the exchange {@code price-updates}, its queues, and the bindings between them. The
 * names and the bindings themselves are in {@link Rules}.
 *
 * <p>The exchange is declared as a <em>fanout</em> exchange: every bound queue gets a copy, and the routing key is
 * ignored. Other types match differently and have their own episodes.
 */
final class Topology {
    private Topology() { }

    /** Safe to call from every side: declaring an exchange that already exists with the same settings does nothing. */
    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(Rules.EXCHANGE, BuiltinExchangeType.FANOUT, true);
    }

    /** A named, durable queue. Declaring a queue that already exists with the same settings does nothing. */
    static void declareQueue(Channel channel, String queue) throws IOException {
        channel.queueDeclare(queue, true, false, false, null);
    }

    /**
     * An <em>exclusive</em> queue: only the connection that declares it can use it, and RabbitMQ deletes it when that
     * connection closes. The empty name asks the broker to choose one; the broker's answer is the name to use.
     * The queue is also non-durable and auto-delete, which is the usual setup for this kind of private queue.
     */
    static String declareExclusiveQueue(Channel channel) throws IOException {
        return channel.queueDeclare("", false, true, true, null).getQueue();
    }

    static void bind(Channel channel, String queue, String bindingKey) throws IOException {
        channel.queueBind(queue, Rules.EXCHANGE, bindingKey);
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
