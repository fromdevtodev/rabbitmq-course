package com.fromdevtodev.rabbitmq.exchanges;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer, {@code order-service}: it publishes {@code count} events with one routing key, numbered from {@code first}.
 * It names the exchange and the routing key. It does not name a queue, and it does not know how many queues exist.
 *
 * <p>It does not ask the broker to confirm anything: publisher confirms are a different mechanism and have their own episode.
 */
final class Publisher {
    private Publisher() { }

    // region readme:publish
    /** Publishes messages {@code first .. first + count - 1} to the exchange, all with the given routing key. */
    static void publish(String routingKey, long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("order-service");
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
                Log.info("order-service", "published " + event.eventId() + " (" + event.orderId() + ") to '"
                        + Topology.EXCHANGE + "' with routing key " + routingKey);
            }
        }
    }
    // endregion
}
