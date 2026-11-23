package com.fromdevtodev.rabbitmq.unroutable;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer, {@code order-service}: it publishes {@code count} order events with one routing key, numbered from {@code first}.
 * With {@code mandatory} it asks the broker to hand back any message that matches no binding, and it listens for the return.
 *
 * <p>Neither publisher confirms nor any consumer-side feature is used here: publisher confirms have their own episode.
 */
final class Publisher {
    private static final long RETURN_GRACE_MILLIS = 700;

    private Publisher() { }

    // region readme:publish
    /** Publishes events {@code first .. first + count - 1} to the exchange orders, all with the given routing key. */
    static void publish(String routingKey, long count, long first, boolean mandatory) throws IOException, TimeoutException, InterruptedException {
        try (Connection connection = Topology.connect("order-service");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            if (mandatory) {
                channel.addReturnListener(Publisher::onReturn);
            }
            for (long seq = first; seq < first + count; seq++) {
                OrderEvent event = OrderEvent.of(seq, routingKey);
                channel.basicPublish(Topology.EXCHANGE, routingKey, mandatory, properties(event), EventJson.toBytes(event));
                Log.info("order-service", "published " + event.eventId() + " (" + event.orderId() + ") to '" + Topology.EXCHANGE
                        + "' with routing key " + routingKey + (mandatory ? ", mandatory" : "") + ": the call returned normally");
            }
            if (mandatory) {
                Thread.sleep(RETURN_GRACE_MILLIS);   // the return arrives after the call has returned: give it a moment
            }
        }
    }
    // endregion

    /** Runs only for a message the broker could not route, and only when it was published with the mandatory flag. */
    // region readme:return
    private static void onReturn(int replyCode, String replyText, String exchange, String routingKey,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        OrderEvent event = EventJson.fromBytes(body);
        Log.info("order-service", "RETURNED " + event.eventId() + ": the broker found no queue for routing key " + routingKey
                + " on '" + exchange + "' (reply " + replyCode + ", " + replyText + ")");
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
