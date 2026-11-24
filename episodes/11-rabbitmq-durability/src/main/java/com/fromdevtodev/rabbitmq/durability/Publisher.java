package com.fromdevtodev.rabbitmq.durability;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer: it publishes {@code count} order events to one queue, numbered from {@code first}, and goes away.
 *
 * <p>The one thing this episode changes is the message's properties. With no properties at all the message is transient; with
 * {@code PERSISTENT_TEXT_PLAIN} it is persistent. Nothing in the call fails either way, which is exactly the problem.
 *
 * <p>It does not ask the broker to confirm anything: publisher confirms are a different mechanism and have their own episode.
 */
final class Publisher {
    private Publisher() { }

    /** Publishes messages {@code first .. first + count - 1} to {@code queue}, in that order. */
    static void publish(String queue, long count, long first, boolean persistent) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("order-service");
             Channel channel = connection.createChannel()) {
            Topology.declareQueue(channel, queue);
            for (long seq = first; seq < first + count; seq++) {
                OrderEvent event = OrderEvent.of(seq);
                send(channel, queue, event, persistent);
                Log.info("order-service", "published " + event.eventId() + " (" + event.orderId() + ") to " + queue
                        + ", " + (persistent ? "persistent" : "transient"));
            }
        }
    }

    // region readme:publish
    /** One message to the default exchange; the queue's name is the routing key. */
    static void send(Channel channel, String queue, OrderEvent event, boolean persistent) throws IOException {
        AMQP.BasicProperties properties = persistent
                ? MessageProperties.PERSISTENT_TEXT_PLAIN   // delivery mode 2: save this message
                : null;                                     // no properties: delivery mode 1, a transient message
        channel.basicPublish("", queue, properties, EventJson.toBytes(event));
    }
    // endregion
}
