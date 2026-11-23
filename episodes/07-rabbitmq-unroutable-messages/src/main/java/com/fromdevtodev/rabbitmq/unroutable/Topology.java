package com.fromdevtodev.rabbitmq.unroutable;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: the direct exchange {@code orders} with three queues, and, for the last experiment, a second
 * exchange {@code orders.unrouted} (fanout) with one queue behind it, {@code unrouted.parking}.
 *
 * <pre>
 *   orders (direct) --order.created-----> notifications.email, loyalty.points, audit.log
 *                   --order.cancelled---> notifications.email, audit.log
 *
 *   orders.unrouted (fanout) ---> unrouted.parking       (attached to orders by a policy, see run.sh: policy)
 * </pre>
 *
 * <p>The producer will publish {@code order.canceled} (one L); every binding uses {@code order.cancelled} (two Ls).
 */
final class Topology {
    static final String EXCHANGE = "orders";
    static final String UNROUTED_EXCHANGE = "orders.unrouted";

    static final String EMAIL = "notifications.email";
    static final String POINTS = "loyalty.points";
    static final String AUDIT = "audit.log";
    static final String PARKING = "unrouted.parking";

    /** Five bindings, two spellings of the cancellation: only the one with two Ls is bound. */
    // region readme:bindings
    static final List<Binding> ORDERS = List.of(
            new Binding(EMAIL, "order.created"),
            new Binding(EMAIL, "order.cancelled"),
            new Binding(POINTS, "order.created"),
            new Binding(AUDIT, "order.created"),
            new Binding(AUDIT, "order.cancelled"));
    // endregion

    private Topology() { }

    /** Declares the exchange. The publisher and the setup both call this, so whoever starts first creates it. */
    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
    }

    /** The starting point of experiments 1 and 2: the exchange orders, its three queues and its five bindings. */
    static void declareOrders(Channel channel) throws IOException {
        declareExchange(channel);
        for (Binding binding : ORDERS) {
            channel.queueDeclare(binding.queue(), true, false, false, null);
            channel.queueBind(binding.queue(), EXCHANGE, binding.key());
        }
    }

    /**
     * The catcher of experiment 3: a fanout exchange with one queue behind it. Declaring it does not attach it to anything: the
     * alternate exchange is attached to {@code orders} afterwards, by a policy ({@code ./run.sh policy}).
     */
    // region readme:parking
    static void declareParking(Channel channel) throws IOException {
        channel.exchangeDeclare(UNROUTED_EXCHANGE, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(PARKING, true, false, false, null);
        channel.queueBind(PARKING, UNROUTED_EXCHANGE, "");
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

    /**
     * Deletes both exchanges and the four queues (and with them the bindings), so an experiment starts from nothing. A policy is not
     * deleted with the exchange it applied to: {@code ./run.sh reset} also clears it.
     */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            for (String queue : List.of(EMAIL, POINTS, AUDIT, PARKING)) {
                channel.queueDelete(queue);
            }
            channel.exchangeDelete(EXCHANGE);
            channel.exchangeDelete(UNROUTED_EXCHANGE);
            Log.info("reset", "exchanges '" + EXCHANGE + "' and '" + UNROUTED_EXCHANGE + "' and their four queues deleted");
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
