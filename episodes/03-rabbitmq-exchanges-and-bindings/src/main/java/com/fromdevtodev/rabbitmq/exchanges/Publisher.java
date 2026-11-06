package com.fromdevtodev.rabbitmq.exchanges;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer. It knows two things: the name of the exchange and the routing key it puts on each message.
 * It never mentions a queue, and it does not know how many queues are bound or who consumes from them.
 *
 * <p>Whatever the bindings are at the moment of publishing decides which queues receive a copy. If no binding
 * matches, the broker drops the message and the publisher is not told (a later episode). The publisher does not
 * ask the broker to confirm each message either; publisher confirms are also a later episode.
 */
final class Publisher {
    private Publisher() { }

    /** Publishes messages {@code first .. first + count - 1}, all with the given routing key. */
    static void run(String routingKey, long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("publisher");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (long seq = first; seq < first + count; seq++) {
                OrderEvent event = OrderEvent.of(seq, routingKey);
                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType("application/json")
                        .messageId(event.eventId())
                        .deliveryMode(2)
                        .build();
                channel.basicPublish(Topology.EXCHANGE, routingKey, properties, EventJson.toBytes(event));
                Log.info("publisher", "published " + event.eventId() + " (" + event.orderId() + ") to exchange '"
                        + Topology.EXCHANGE + "' with routing key '" + routingKey + "'");
            }
        }
    }
}
