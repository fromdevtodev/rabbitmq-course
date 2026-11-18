package com.fromdevtodev.rabbitmq.direct;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode: one direct exchange, three queues and five bindings.
 *
 * <pre>
 *                      +--error-------------> errors-pager
 *   log-service --> [logs] --info, warning, error--> audit
 *                      +--warning-----------> metrics
 * </pre>
 *
 * <p>The second demo uses a fourth queue, {@code notifications.email}, which has no binding at all: the only exchange it is
 * bound to is the default exchange, and the broker made that binding.
 */
final class Topology {
    static final String EXCHANGE = "logs";
    static final String PAGER = "errors-pager";
    static final String AUDIT = "audit";
    static final String METRICS = "metrics";
    static final String EMAIL = "notifications.email";

    /** The five bindings of the exchange {@code logs}: audit is bound three times, once per key. */
    // region readme:bindings
    static final List<Binding> BINDINGS = List.of(
            new Binding(PAGER, LogEvent.ERROR),
            new Binding(AUDIT, LogEvent.INFO),
            new Binding(AUDIT, LogEvent.WARNING),
            new Binding(AUDIT, LogEvent.ERROR),
            new Binding(METRICS, LogEvent.WARNING));
    // endregion

    private Topology() { }

    /**
     * Declares the exchange. The publisher and the setup both call this, so whoever starts first creates it.
     */
    // region readme:exchange
    static void declareExchange(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
    }
    // endregion

    /** Declares a queue and binds it to the exchange with one binding key: the two calls every consumer-side service makes. */
    // region readme:bind
    static void declareAndBind(Channel channel, String queue, String bindingKey) throws IOException {
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, EXCHANGE, bindingKey);
    }
    // endregion

    /** The starting point of the first demo: the exchange, the three queues and the five bindings. */
    static void declareAll(Channel channel) throws IOException {
        declareExchange(channel);
        for (Binding binding : BINDINGS) {
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

    /** Deletes the exchange and the four queues (and with them the bindings), so a demo starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(PAGER);
            channel.queueDelete(AUDIT);
            channel.queueDelete(METRICS);
            channel.queueDelete(EMAIL);
            channel.exchangeDelete(EXCHANGE);
            Log.info("reset", "exchange '" + EXCHANGE + "' and its four queues deleted");
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
