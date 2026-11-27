package com.fromdevtodev.rabbitmq.retry;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/** The producer, {@code order-service}: it publishes {@code count} orders, numbered from {@code first}. No publisher confirms (another episode). */
final class Publisher {
    private Publisher() { }

    static void publish(long first, long count) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("order-service");
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            for (long seq = first; seq < first + count; seq++) {
                OrderEvent event = OrderEvent.of(seq);
                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType("application/json").messageId(event.eventId()).deliveryMode(2).build();
                channel.basicPublish(Topology.EXCHANGE, Topology.ROUTING_KEY, properties, EventJson.toBytes(event));
                Log.info("order-service", "published " + event.eventId() + " (" + event.orderId() + ")");
            }
        }
    }
}
