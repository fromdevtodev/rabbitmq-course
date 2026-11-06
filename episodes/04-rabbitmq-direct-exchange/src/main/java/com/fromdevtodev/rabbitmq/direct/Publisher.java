package com.fromdevtodev.rabbitmq.direct;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer. It knows two things: an exchange name and the routing key it puts on each message. It never mentions a
 * queue in {@link #toExchange}.
 *
 * <p>If no binding matches the key, the broker discards the message and the publisher is not told (a later episode). The
 * publisher does not ask the broker to confirm each message either; publisher confirms are also a later episode.
 */
final class Publisher {
    private Publisher() { }

    /** Publishes messages {@code first .. first + count - 1} to the exchange {@code logs}, all with the same key. */
    static void toExchange(String routingKey, long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("publisher");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (long seq = first; seq < first + count; seq++) {
                LogEvent event = LogEvent.of(seq, routingKey);
                channel.basicPublish(Rules.EXCHANGE, routingKey, properties(event), EventJson.toBytes(event));
                Log.info("publisher", "published " + event.eventId() + " to exchange '" + Rules.EXCHANGE
                        + "' with routing key '" + routingKey + "'");
            }
        }
    }

    /**
     * Publishes to the <em>default</em> exchange: the exchange name is the empty string and the routing key is the name of a
     * queue. It looks like sending to the queue, but it is still an exchange, with a binding RabbitMQ made by itself.
     */
    static void toDefaultExchange(String queue, long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("publisher-default");
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel, queue);
            for (long seq = first; seq < first + count; seq++) {
                LogEvent event = LogEvent.of(seq, "info");
                channel.basicPublish("", queue, properties(event), EventJson.toBytes(event));
                Log.info("publisher", "published " + event.eventId() + " to the default exchange ('') with routing key '"
                        + queue + "'");
            }
        }
    }

    private static AMQP.BasicProperties properties(LogEvent event) {
        return new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .messageId(event.eventId())
                .deliveryMode(2)
                .build();
    }
}
