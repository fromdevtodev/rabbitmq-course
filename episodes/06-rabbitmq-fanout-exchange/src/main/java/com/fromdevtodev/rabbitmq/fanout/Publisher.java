package com.fromdevtodev.rabbitmq.fanout;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer, {@code pricing-service}: it publishes {@code count} price changes with one routing key, numbered from
 * {@code first}. It names an exchange and a key, and it does not know how many queues are bound, how many consumers listen or
 * what the bindings say.
 *
 * <p>The publisher does not ask the broker to confirm anything and does not set the {@code mandatory} flag: a message that
 * reaches no queue is discarded without an error. Publisher confirms and unroutable messages have their own episodes.
 */
final class Publisher {
    private Publisher() { }

    // region readme:publish
    /** Publishes price changes {@code first .. first + count - 1} to the exchange price-updates, all with the given key. */
    static void publish(String routingKey, long count, long first) throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("pricing-service");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            for (long seq = first; seq < first + count; seq++) {
                PriceUpdate update = PriceUpdate.of(seq, routingKey);
                channel.basicPublish(Topology.EXCHANGE, routingKey, properties(update), EventJson.toBytes(update));
                Log.info("pricing-service", "published " + update.eventId() + " to '" + Topology.EXCHANGE
                        + "' with routing key " + routingKey);
            }
        }
    }
    // endregion

    private static AMQP.BasicProperties properties(PriceUpdate update) {
        return new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .messageId(update.eventId())
                .deliveryMode(2)
                .build();
    }
}
