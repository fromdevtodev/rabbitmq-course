package com.fromdevtodev.rabbitmq.exchanges;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: one exchange ({@code orders}), three queues, and the bindings between them.
 *
 * <p>The exchange is declared as a <em>direct</em> exchange: a binding matches when its routing key equals the
 * routing key of the message. Other exchange types match differently; they have their own episodes. Nothing in this
 * project depends on knowing more than that.
 */
final class Topology {
    static final String EXCHANGE = "orders";

    static final String EMAIL = "notifications.email";
    static final String POINTS = "loyalty.points";
    static final String AUDIT = "audit.log";
    static final List<String> QUEUES = List.of(EMAIL, POINTS, AUDIT);

    static final String ORDER_CREATED = "order.created";
    static final String ORDER_CANCELLED = "order.cancelled";

    /** One binding: "this queue wants the messages of the {@code orders} exchange that carry this routing key". */
    record Binding(String queue, String routingKey) { }

    /**
     * The bindings that exist at the start of the story. {@code loyalty.points} is missing on purpose: the points worker
     * adds its own queue and binding while the order service keeps running, unchanged.
     */
    static final List<Binding> INITIAL_BINDINGS = List.of(
            new Binding(EMAIL, ORDER_CREATED),
            new Binding(AUDIT, ORDER_CANCELLED));

    private Topology() { }

    /** Safe to call from every side: declaring an exchange that already exists with the same settings does nothing. */
    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
    }

    /** Declaring a queue that already exists with the same settings does nothing. */
    static void declareQueue(Channel channel, String queue) throws IOException {
        channel.queueDeclare(queue, true, false, false, null);
    }

    /**
     * What a consumer does when it starts: make sure the exchange and its own queue exist, then bind the queue.
     * The publisher is not involved.
     */
    static void bind(Channel channel, String queue, String routingKey) throws IOException {
        declareExchange(channel);
        declareQueue(channel, queue);
        channel.queueBind(queue, EXCHANGE, routingKey);
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
