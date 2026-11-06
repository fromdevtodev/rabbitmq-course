package com.fromdevtodev.rabbitmq.nack;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer. It publishes {@code order.created} events to the exchange {@code orders} and never mentions a queue.
 * One of the messages can be published <em>without an email address</em>: that is the message the worker cannot process.
 */
final class Publisher {
    private Publisher() { }

    /**
     * Publishes messages {@code first .. first + count - 1}. The message whose number equals {@code poison} has no
     * email address ({@code poison <= 0}: none).
     */
    static void toExchange(long first, long count, long poison) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("publisher");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (long seq = first; seq < first + count; seq++) {
                OrderEvent event = OrderEvent.of(seq, seq == poison);
                channel.basicPublish(Rules.EXCHANGE, Rules.BINDING_KEY, properties(event), EventJson.toBytes(event));
                Log.info("publisher", "published " + event.eventId() + " for " + event.orderId()
                        + (event.hasEmail() ? "" : " (no email address)"));
            }
        }
    }

    private static AMQP.BasicProperties properties(OrderEvent event) {
        return new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .messageId(event.eventId())
                .deliveryMode(2)
                .build();
    }
}
