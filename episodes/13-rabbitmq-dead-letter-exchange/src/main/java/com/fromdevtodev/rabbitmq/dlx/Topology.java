package com.fromdevtodev.rabbitmq.dlx;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode. From the earlier episodes: the direct exchange {@code orders}, the queue
 * {@code notifications.email} and its binding with the key {@code order.created}. New: the dead-letter exchange
 * {@code orders.dlx}, the dead-letter queue {@code notifications.email.dlq}, and one queue argument that connects them.
 * Every queue is declared as a classic queue explicitly, so the demos do not depend on the default queue type.
 */
final class Topology {
    static final String EXCHANGE = "orders";
    static final String QUEUE = "notifications.email";
    static final String ROUTING_KEY = "order.created";
    static final String DEAD_LETTER_EXCHANGE = "orders.dlx";
    static final String DEAD_LETTER_QUEUE = "notifications.email.dlq";

    private Topology() { }

    /** Step 1 of the build: where failures will go. {@code bindingKey} is the key the dead-letter queue is bound with. */
    // region readme:dead-letter-side
    static void declareDeadLetterSide(Channel channel, String bindingKey) throws IOException {
        channel.exchangeDeclare(DEAD_LETTER_EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(DEAD_LETTER_QUEUE, true, false, false, Map.of("x-queue-type", "classic"));
        channel.queueBind(DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, bindingKey);
    }
    // endregion

    /** Step 2 of the build: the work queue, which names the dead-letter exchange. */
    // region readme:source-side
    static void declareSourceSide(Channel channel, Options options) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-queue-type", "classic");
        if (options.deadLetter()) {
            arguments.put("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE);   // no x-dead-letter-routing-key: the key is kept
        }
        if (options.maxLength() > 0) {
            arguments.put("x-max-length", options.maxLength());             // README experiment: reason "maxlen"
        }
        if (options.messageTtlMillis() > 0) {
            arguments.put("x-message-ttl", options.messageTtlMillis());     // README experiment: reason "expired"
        }
        channel.queueDeclare(QUEUE, true, false, false, arguments);
        channel.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);
    }
    // endregion

    /** Settings of the work queue that the experiments change. */
    record Options(boolean deadLetter, long maxLength, long messageTtlMillis) {
        static Options defaults() {
            return new Options(true, 0, 0);
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

    /** Deletes both queues and both exchanges, so an experiment starts from nothing. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(QUEUE);
            channel.queueDelete(DEAD_LETTER_QUEUE);
            channel.exchangeDelete(EXCHANGE);
            channel.exchangeDelete(DEAD_LETTER_EXCHANGE);
            Log.info("reset", "queues '" + QUEUE + "', '" + DEAD_LETTER_QUEUE + "' and exchanges '" + EXCHANGE + "', '"
                    + DEAD_LETTER_EXCHANGE + "' deleted");
        }
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
