package com.fromdevtodev.rabbitmq.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: one fanout exchange, {@code price-updates}, and the queues of two experiments.
 *
 * <pre>
 *   experiment 1                                    experiment 2
 *   price-updates --""-----------> cache.instance-1   price-updates --""--> cache.shared (consumers cache-a, cache-b)
 *                 --books---------> cache.instance-2                  --""--> one exclusive queue per instance,
 *                 --electronics---> cache.instance-3                          named by the broker
 * </pre>
 *
 * <p>The binding keys of experiment 1 are different on purpose: a fanout exchange ignores all of them.
 */
final class Topology {
    static final String EXCHANGE = "price-updates";

    static final String INSTANCE_1 = "cache.instance-1";
    static final String INSTANCE_2 = "cache.instance-2";
    static final String INSTANCE_3 = "cache.instance-3";
    static final String SHARED = "cache.shared";

    /** Experiment 1: three queues, three different binding keys. A fanout exchange treats them all the same. */
    // region readme:bindings
    static final List<Binding> BROADCAST = List.of(
            new Binding(INSTANCE_1, ""),
            new Binding(INSTANCE_2, "books"),
            new Binding(INSTANCE_3, "electronics"));
    // endregion

    private Topology() { }

    /** Declares the exchange. The publisher and the setup both call this, so whoever starts first creates it. */
    // region readme:exchange
    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT, true);
    }
    // endregion

    /** Declares a named queue and binds it with a key: the two calls every receiver makes. The key changes nothing. */
    // region readme:bind
    static void declareAndBind(Channel channel, String queue, String key) throws IOException {
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, EXCHANGE, key);
    }
    // endregion

    /**
     * Declares a queue of this instance alone and binds it. An empty name asks the broker to choose one, and the client uses
     * the name that comes back. The queue is exclusive (only this connection can use it) and the broker deletes it when the
     * connection closes.
     */
    // region readme:exclusive
    static String declareExclusiveAndBind(Channel channel) throws IOException {
        String queue = channel.queueDeclare("", false, true, true, null).getQueue();
        channel.queueBind(queue, EXCHANGE, "");
        return queue;
    }
    // endregion

    /** The starting point of experiment 1: the exchange, and one queue and one binding for each entry of the table. */
    static void declareAll(Channel channel, List<Binding> bindings) throws IOException {
        declareExchange(channel);
        for (Binding binding : bindings) {
            declareAndBind(channel, binding.queue(), binding.key());
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

    /** Deletes the exchange and the four named queues (and with them the bindings), so an experiment starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            for (Binding binding : BROADCAST) {
                channel.queueDelete(binding.queue());
            }
            channel.queueDelete(SHARED);
            channel.exchangeDelete(EXCHANGE);
            Log.info("reset", "exchange '" + EXCHANGE + "' and its four named queues deleted");
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
