package com.fromdevtodev.rabbitmq.unroutable;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: the direct exchange {@code orders}, its queues and bindings, and the optional
 * alternate exchange with the queue behind it.
 *
 * <p>Nothing here sets the alternate-exchange <em>policy</em> on {@code orders}: that is a broker setting you apply with
 * {@code rabbitmqctl set_policy} (see the README), so the producer and this code never change.
 */
final class Topology {
    private Topology() { }

    /** Safe to call from every side: declaring an exchange that already exists with the same settings does nothing. */
    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(Rules.EXCHANGE, BuiltinExchangeType.DIRECT, true);
    }

    static void declareQueue(Channel channel, String queue) throws IOException {
        channel.queueDeclare(queue, true, false, false, null);
    }

    static void bind(Channel channel, String queue, String bindingKey) throws IOException {
        channel.queueBind(queue, Rules.EXCHANGE, bindingKey);
    }

    /** The alternate exchange is a fanout exchange: it copies every message it receives, whatever the key. */
    static void declareAlternate(Channel channel) throws IOException {
        channel.exchangeDeclare(Rules.ALTERNATE_EXCHANGE, BuiltinExchangeType.FANOUT, true);
        declareQueue(channel, Rules.PARKING_QUEUE);
        channel.queueBind(Rules.PARKING_QUEUE, Rules.ALTERNATE_EXCHANGE, "");
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
