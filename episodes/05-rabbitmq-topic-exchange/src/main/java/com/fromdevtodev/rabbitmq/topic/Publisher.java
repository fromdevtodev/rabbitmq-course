package com.fromdevtodev.rabbitmq.topic;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer, {@code event-service}: it publishes {@code count} events with one routing key, numbered from {@code first}.
 * It names an exchange and a routing key, and it does not know how many queues exist, which patterns they use or whether
 * any of them matches.
 *
 * <p>The routing key is a plain list of words: wildcards belong to the bindings, never to the message. The publisher does not
 * ask the broker to confirm anything and does not set the {@code mandatory} flag: a message whose key matches no pattern is
 * discarded without an error. Publisher confirms and unroutable messages have their own episodes.
 */
final class Publisher {
    private Publisher() { }

    // region readme:publish
    /** Publishes events {@code first .. first + count - 1} to the exchange events, all with the given routing key. */
    static void publish(String routingKey, long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("event-service");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (long seq = first; seq < first + count; seq++) {
                DomainEvent event = DomainEvent.of(seq, routingKey);
                channel.basicPublish(Topology.EXCHANGE, routingKey, properties(event), EventJson.toBytes(event));
                Log.info("event-service", "published " + event.eventId() + " to '" + Topology.EXCHANGE
                        + "' with routing key " + routingKey);
            }
        }
    }
    // endregion

    private static AMQP.BasicProperties properties(DomainEvent event) {
        return new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .messageId(event.eventId())
                .deliveryMode(2)
                .build();
    }
}
