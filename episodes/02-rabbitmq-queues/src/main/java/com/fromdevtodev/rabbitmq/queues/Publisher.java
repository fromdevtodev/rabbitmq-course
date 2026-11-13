package com.fromdevtodev.rabbitmq.queues;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer: it publishes {@code count} order events to the queue, numbered from {@code first}, and goes away. It does not
 * know whether a consumer exists, and it does not wait for one.
 *
 * <p>It does not ask the broker to confirm anything: publisher confirms are a different mechanism and have their own episode.
 */
final class Publisher {
    private Publisher() { }

    // region readme:publish
    /** Publishes messages {@code first .. first + count - 1}, in that order. */
    static void publish(long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("order-service");
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
                Log.info("order-service", "published " + event.eventId() + " (" + event.orderId() + ")");
            }
        }
    }
    // endregion
}
