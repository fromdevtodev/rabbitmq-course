package com.fromdevtodev.rabbitmq.whyabroker;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer side: one line in the controller, {@code publisher.orderCreated(order)}.
 *
 * <p>It returns as soon as the client has handed the message over. It does not wait for the consumer, and it does
 * not ask the broker to confirm it stored the message (publisher confirms are a later episode).
 */
public final class RabbitOrderEventPublisher implements OrderEventPublisher, AutoCloseable {
    private final Connection connection;
    private final Channel channel;

    public RabbitOrderEventPublisher() throws IOException, TimeoutException {
        this.connection = Topology.connect("order-service");
        this.channel = connection.createChannel();
        Topology.declare(channel);
    }

    @Override
    public void orderCreated(Order order) throws IOException {
        OrderCreated event = OrderCreated.from(order);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .messageId(event.eventId())
                .deliveryMode(2) // persistent: asks the broker to write it to disk; durability details come later
                .build();
        // A Channel is not safe for concurrent use, and the HTTP server calls this from several threads.
        synchronized (channel) {
            channel.basicPublish("", Topology.QUEUE, properties, EventJson.toBytes(event));
        }
        Log.info("order-service", "published " + event.eventId() + " (" + event.type() + ", " + order.id() + ")");
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
