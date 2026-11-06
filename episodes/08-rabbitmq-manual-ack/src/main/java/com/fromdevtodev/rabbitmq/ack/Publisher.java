package com.fromdevtodev.rabbitmq.ack;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer: it publishes {@code count} order events to the default exchange, with the queue name as routing key.
 *
 * <p>It does not ask the broker to confirm anything: publisher confirms are a different mechanism from the consumer
 * acknowledgements of this episode, and have their own episode.
 */
final class Publisher {
    private Publisher() { }

    /** Publishes messages {@code first .. first + count - 1}. */
    static void publish(long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("publisher");
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel);
            for (long seq = first; seq < first + count; seq++) {
                OrderEvent event = OrderEvent.of(seq);
                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType("application/json")
                        .messageId(event.eventId())
                        .deliveryMode(2)
                        .build();
                channel.basicPublish("", Topology.QUEUE, properties, EventJson.toBytes(event));
                Log.info("publisher", "published " + event.eventId() + " (" + event.orderId() + ") to '" + Topology.QUEUE + "'");
            }
        }
    }
}
