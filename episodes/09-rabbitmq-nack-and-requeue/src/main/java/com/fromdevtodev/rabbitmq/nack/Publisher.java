package com.fromdevtodev.rabbitmq.nack;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * The producer, {@code order-service}: it publishes {@code count} orders, numbered from {@code first}. The events whose number is in
 * {@code withoutEmail} are published without an email address: those are the bad messages of the episode.
 *
 * <p>The publisher does not ask the broker to confirm anything: publisher confirms are another episode.
 */
final class Publisher {
    private Publisher() { }

    // region readme:publish
    static void publish(long first, long count, Set<Long> withoutEmail) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("order-service");
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            for (long seq = first; seq < first + count; seq++) {
                OrderEvent event = OrderEvent.of(seq, withoutEmail.contains(seq));
                channel.basicPublish(Topology.EXCHANGE, Topology.ROUTING_KEY, properties(event), EventJson.toBytes(event));
                Log.info("order-service", "published " + event.eventId() + " (" + event.orderId() + ")"
                        + (event.hasEmailAddress() ? "" : ", without an email address"));
            }
        }
    }
    // endregion

    private static AMQP.BasicProperties properties(OrderEvent event) {
        return new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .messageId(event.eventId())
                .deliveryMode(2)
                .build();
    }
}
