package com.fromdevtodev.rabbitmq.broker;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The order service's side of the broker. The connection is opened once and kept, like a real service would do, so the time
 * of one checkout does not include a connection handshake.
 */
final class RabbitPublisher implements EventPublisher, AutoCloseable {
    private final Connection connection;
    private final Channel channel;

    RabbitPublisher() throws IOException, TimeoutException {
        this.connection = Topology.connect("order-service");
        this.channel = connection.createChannel();
        Topology.declareQueue(channel);
    }

    // region readme:publish
    @Override
    public void publish(OrderEvent event) throws IOException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .messageId(event.eventId())
                .deliveryMode(2)
                .build();
        channel.basicPublish("", Topology.QUEUE, properties, EventJson.toBytes(event));
    }
    // endregion

    @Override
    public void close() throws IOException, TimeoutException {
        channel.close();
        connection.close();
    }
}
