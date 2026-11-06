package com.fromdevtodev.rabbitmq.unroutable;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The producer. It knows an exchange name, the routing key it puts on each message and, optionally, the
 * <em>mandatory</em> flag. It never mentions a queue.
 *
 * <p>Without the flag, a message that matches no binding is discarded and the publisher is not told. With the flag,
 * the broker returns it, and the return listener below prints it. If the exchange has an alternate exchange that can
 * route the message, the message counts as routed and nothing is returned.
 *
 * <p>The publisher does not ask the broker to confirm each message; publisher confirms are a later episode.
 */
final class Publisher {
    /** How long to wait for a possible return. Returns arrive asynchronously, after the publish call has returned. */
    static final long RETURN_WAIT_MILLIS = 1_000;

    private Publisher() { }

    /** Publishes messages {@code first .. first + count - 1} to {@code orders}, all with the same routing key. */
    static void toExchange(String routingKey, boolean mandatory, long count, long first)
            throws IOException, TimeoutException, InterruptedException {
        try (Connection connection = Topology.connect("publisher");
             Channel channel = connection.createChannel()) {
            Topology.declareExchange(channel);
            AtomicLong returned = new AtomicLong();
            CountDownLatch gotReturn = new CountDownLatch((int) count);
            if (mandatory) {
                channel.addReturnListener((replyCode, replyText, exchange, key, properties, body) -> {
                    returned.incrementAndGet();
                    Log.info("publisher", "RETURNED " + properties.getMessageId() + ": reply code " + replyCode
                            + ", reply text " + replyText + ", exchange '" + exchange + "', routing key '" + key + "'");
                    gotReturn.countDown();
                });
            }
            for (long seq = first; seq < first + count; seq++) {
                OrderEvent event = OrderEvent.of(seq, routingKey);
                channel.basicPublish(Rules.EXCHANGE, routingKey, mandatory, properties(event), EventJson.toBytes(event));
                Log.info("publisher", "published " + event.eventId() + " to exchange '" + Rules.EXCHANGE
                        + "' with routing key '" + routingKey + "'" + (mandatory ? " (mandatory)" : ""));
            }
            if (mandatory) {
                gotReturn.await(RETURN_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                Log.info("publisher", returned.get() == 0
                        ? "no message was returned"
                        : returned.get() + " message(s) were returned");
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
