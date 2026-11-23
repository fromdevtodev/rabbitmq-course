package com.fromdevtodev.rabbitmq.exchanges;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: one exchange, three queues and the bindings between them.
 *
 * <pre>
 *   order-service --order.created---> [orders] --order.created---> notifications.email
 *                 --order.cancelled-> [orders] --order.created---> loyalty.points   (added during the episode)
 *                                              --order.cancelled-> audit.log
 * </pre>
 *
 * <p>The exchange is a <em>direct</em> exchange: a binding matches when its key equals the routing key of the message. How
 * each exchange type matches is the subject of the next episodes; here only that "a binding decides" matters.
 */
final class Topology {
    static final String EXCHANGE = "orders";
    static final String EMAIL = "notifications.email";
    static final String POINTS = "loyalty.points";
    static final String AUDIT = "audit.log";

    private Topology() { }

    /**
     * Declares the exchange. The publisher and every worker call this, so whoever starts first creates it.
     */
    // region readme:exchange
    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
    }
    // endregion

    /** Declares a queue and binds it to the exchange: the two calls every consumer-side service makes. */
    // region readme:bind
    static void declareAndBind(Channel channel, String queue, String bindingKey) throws IOException {
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, EXCHANGE, bindingKey);
    }
    // endregion

    /** The two queues that exist before the episode starts: email gets new orders, audit gets cancellations. */
    static void declareInitialQueues(Channel channel) throws IOException {
        declareExchange(channel);
        declareAndBind(channel, EMAIL, OrderEvent.CREATED);
        declareAndBind(channel, AUDIT, OrderEvent.CANCELLED);
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

    /** Deletes the exchange and the three queues (and with them the bindings), so a demo starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(EMAIL);
            channel.queueDelete(POINTS);
            channel.queueDelete(AUDIT);
            channel.exchangeDelete(EXCHANGE);
            Log.info("reset", "exchange '" + EXCHANGE + "' and its three queues deleted");
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
