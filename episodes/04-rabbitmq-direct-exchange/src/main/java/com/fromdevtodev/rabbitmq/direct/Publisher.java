package com.fromdevtodev.rabbitmq.direct;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer, {@code log-service}: it publishes {@code count} log lines with one routing key, numbered from {@code first}.
 * It names an exchange and a routing key, and it does not know how many queues exist or which ones are bound.
 *
 * <p>It does not ask the broker to confirm anything and does not set the {@code mandatory} flag: a message whose key matches
 * no binding is discarded without an error. Publisher confirms and unroutable messages have their own episodes.
 */
final class Publisher {
    private Publisher() { }

    // region readme:publish
    /** Publishes lines {@code first .. first + count - 1} to the exchange logs, all with the given routing key. */
    static void publish(String routingKey, long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("log-service");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (long seq = first; seq < first + count; seq++) {
                LogEvent event = LogEvent.of(seq, routingKey);
                channel.basicPublish(Topology.EXCHANGE, routingKey, properties(event), EventJson.toBytes(event));
                Log.info("log-service", "published " + event.eventId() + " to '" + Topology.EXCHANGE
                        + "' with routing key " + routingKey);
            }
        }
    }
    // endregion

    // region readme:publish-default
    /** The same publish, to the default exchange: the exchange name is empty and the routing key is the name of a queue. */
    static void publishToDefault(String queueName, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("log-service");
             Channel channel = connection.createChannel()) {
            LogEvent event = LogEvent.of(first, LogEvent.INFO);
            channel.basicPublish("", queueName, properties(event), EventJson.toBytes(event));
            Log.info("log-service", "published " + event.eventId() + " to the default exchange ('') with routing key "
                    + queueName);
        }
    }
    // endregion

    private static AMQP.BasicProperties properties(LogEvent event) {
        return new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .messageId(event.eventId())
                .deliveryMode(2)
                .build();
    }
}
