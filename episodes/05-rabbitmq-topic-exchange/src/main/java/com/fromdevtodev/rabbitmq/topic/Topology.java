package com.fromdevtodev.rabbitmq.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: one topic exchange, {@code events}, and two sets of queues, one for each experiment.
 *
 * <pre>
 *   experiment 1                                         experiment 2
 *   events --orders.*.created--> created-orders          events --orders.*-----> orders-star
 *          --orders.#----------> all-orders                     --orders.#-----> orders-hash
 *          --*.eu.*-----------> eu-audit                        --ord*.eu.created--> orders-glob
 *          --payments.#-------> payments
 * </pre>
 *
 * <p>Both sets are bound to the same exchange. They never exist at the same time: each experiment starts with {@code reset}.
 */
final class Topology {
    static final String EXCHANGE = "events";

    static final String CREATED_ORDERS = "created-orders";
    static final String ALL_ORDERS = "all-orders";
    static final String EU_AUDIT = "eu-audit";
    static final String PAYMENTS = "payments";

    static final String ORDERS_STAR = "orders-star";
    static final String ORDERS_HASH = "orders-hash";
    static final String ORDERS_GLOB = "orders-glob";

    /** Experiment 1: four queues, four patterns. Each pattern is a list of words, and a word can be a wildcard. */
    // region readme:patterns
    static final List<Binding> ROUTING = List.of(
            new Binding(CREATED_ORDERS, "orders.*.created"),
            new Binding(ALL_ORDERS, "orders.#"),
            new Binding(EU_AUDIT, "*.eu.*"),
            new Binding(PAYMENTS, "payments.#"));
    // endregion

    /** Experiment 2: star, hash, and a star inside a word. */
    // region readme:comparison
    static final List<Binding> COMPARISON = List.of(
            new Binding(ORDERS_STAR, "orders.*"),
            new Binding(ORDERS_HASH, "orders.#"),
            new Binding(ORDERS_GLOB, "ord*.eu.created"));
    // endregion

    private Topology() { }

    /** Declares the exchange. The publisher and the setup both call this, so whoever starts first creates it. */
    // region readme:exchange
    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, true);
    }
    // endregion

    /** Declares a queue and binds it with one pattern: the two calls every consumer-side service makes. */
    // region readme:bind
    static void declareAndBind(Channel channel, String queue, String pattern) throws IOException {
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, EXCHANGE, pattern);
    }
    // endregion

    /** The starting point of an experiment: the exchange, and one queue and one binding for each entry of the table. */
    static void declareAll(Channel channel, List<Binding> bindings) throws IOException {
        declareExchange(channel);
        for (Binding binding : bindings) {
            declareAndBind(channel, binding.queue(), binding.pattern());
        }
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

    /** Deletes the exchange and the seven queues (and with them the bindings), so an experiment starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            for (Binding binding : ROUTING) {
                channel.queueDelete(binding.queue());
            }
            for (Binding binding : COMPARISON) {
                channel.queueDelete(binding.queue());
            }
            channel.exchangeDelete(EXCHANGE);
            Log.info("reset", "exchange '" + EXCHANGE + "' and its seven queues deleted");
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
