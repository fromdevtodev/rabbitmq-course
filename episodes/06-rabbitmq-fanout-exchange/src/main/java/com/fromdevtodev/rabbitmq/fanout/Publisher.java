package com.fromdevtodev.rabbitmq.fanout;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer. It knows an exchange name and puts a routing key on each message; it never mentions a queue. With a
 * fanout exchange the routing key changes nothing, and the producer does not know how many queues are bound.
 *
 * <p>If no queue is bound, the broker discards the message and the publisher is not told (a later episode). The
 * publisher does not ask the broker to confirm each message either; publisher confirms are also a later episode.
 */
final class Publisher {
    private Publisher() { }

    /** Publishes messages {@code first .. first + count - 1} to the exchange {@code price-updates}, all with the same key. */
    static void toExchange(String routingKey, long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("publisher");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (long seq = first; seq < first + count; seq++) {
                PriceChange event = PriceChange.of(seq, routingKey);
                channel.basicPublish(Rules.EXCHANGE, routingKey, properties(event), EventJson.toBytes(event));
                Log.info("publisher", "published " + event.eventId() + " to exchange '" + Rules.EXCHANGE
                        + "' with routing key '" + routingKey + "'");
            }
        }
    }

    private static AMQP.BasicProperties properties(PriceChange event) {
        return new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .messageId(event.eventId())
                .deliveryMode(2)
                .build();
    }
}
