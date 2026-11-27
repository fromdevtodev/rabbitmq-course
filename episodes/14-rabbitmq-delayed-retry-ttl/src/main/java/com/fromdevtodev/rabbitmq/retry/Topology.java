package com.fromdevtodev.rabbitmq.retry;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * The RabbitMQ objects of this episode. {@link #declare} is the starting point (the work queue, as in episode 9);
 * {@link #declareRetry} adds the retry loop. The policy that sends a rejected message to the retry exchange is NOT set here: the
 * Java client cannot set policies, so {@code ./run.sh build} sets it with {@code rabbitmqctl}. Queues are classic, declared explicitly.
 */
final class Topology {
    static final String EXCHANGE = "orders";
    static final String QUEUE = "notifications.email";
    static final String ROUTING_KEY = "order.created";
    static final String RETRY_EXCHANGE = "orders.retry";
    static final String RETRY_QUEUE = "notifications.email.retry";

    private Topology() { }

    static void declare(Channel channel) throws IOException {
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(QUEUE, true, false, false, Map.of("x-queue-type", "classic"));
        channel.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);
    }

    // region readme:retry-topology
    static void declareRetry(Channel channel, long delayMillis) throws IOException {
        channel.exchangeDeclare(RETRY_EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(RETRY_QUEUE, true, false, false, Map.of(
                "x-queue-type", "classic",
                "x-message-ttl", delayMillis,             // how long a message waits here
                "x-dead-letter-exchange", EXCHANGE));     // where it goes when the time is up
        channel.queueBind(RETRY_QUEUE, RETRY_EXCHANGE, ROUTING_KEY);
    }
    // endregion

    /** True when the retry queue exists (a passive declare on a channel of its own, because a failed one closes the channel). */
    static boolean retryQueueExists(Connection connection) throws IOException {
        try (Channel probe = connection.createChannel()) {
            probe.queueDeclarePassive(RETRY_QUEUE);
            return true;
        } catch (IOException | TimeoutException missing) {
            return false;
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
        return factory.newConnection(connectionName);
    }

    /** Deletes both queues and both exchanges (and with them the bindings and every message). The policy is cleared by run.sh. */
    static void reset() throws IOException, TimeoutException {
        try (Connection connection = connect("reset");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(QUEUE);
            channel.queueDelete(RETRY_QUEUE);
            channel.exchangeDelete(EXCHANGE);
            channel.exchangeDelete(RETRY_EXCHANGE);
            Log.info("reset", "queues '" + QUEUE + "' and '" + RETRY_QUEUE + "' and exchanges '" + EXCHANGE + "' and '" + RETRY_EXCHANGE + "' deleted");
        }
    }

    /** The retry delay of the demos: the message TTL of the retry queue (10 s unless RETRY_DELAY_MS says otherwise). */
    static long retryDelayMillis() {
        return Long.parseLong(setting("RETRY_DELAY_MS", "10000"));
    }

    private static String setting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
