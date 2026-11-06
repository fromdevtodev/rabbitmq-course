package com.fromdevtodev.rabbitmq.topic;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer. It knows an exchange name and the routing key it puts on each message. The key is always a plain key
 * such as {@code orders.eu.created}: wildcards belong to bindings, never to the message. It does not mention a queue.
 *
 * <p>If no binding matches the key, the broker discards the message and the publisher is not told (a later episode). The
 * publisher does not ask the broker to confirm each message either; publisher confirms are also a later episode.
 */
final class Publisher {
    private Publisher() { }

    /** Publishes messages {@code first .. first + count - 1} to the exchange {@code events}, all with the same key. */
    static void run(String routingKey, long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("publisher");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (long seq = first; seq < first + count; seq++) {
                Event event = Event.of(seq, routingKey);
                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType("application/json")
                        .messageId(event.eventId())
                        .deliveryMode(2)
                        .build();
                channel.basicPublish(Rules.EXCHANGE, routingKey, properties, EventJson.toBytes(event));
                Log.info("publisher", "published " + event.eventId() + " to exchange '" + Rules.EXCHANGE
                        + "' with routing key '" + routingKey + "'");
            }
        }
    }
}
