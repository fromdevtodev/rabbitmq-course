package com.fromdevtodev.rabbitmq.dlx;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Looks at the first message of the dead-letter queue and prints why it died, then puts it back (a peek: nack with requeue true on
 * the dead-letter queue, which has no dead-letter exchange of its own). The message stays where it was.
 */
final class Inspector {
    private Inspector() { }

    // region readme:inspect
    static void inspect() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("inspector");
             Channel channel = connection.createChannel()) {
            GetResponse response = channel.basicGet(Topology.DEAD_LETTER_QUEUE, false);
            if (response == null) {
                Log.info("inspector", "'" + Topology.DEAD_LETTER_QUEUE + "' is empty");
                return;
            }
            OrderEvent order = EventJson.fromBytes(response.getBody());
            List<XDeath.Death> deaths = XDeath.parse(response.getProps().getHeaders() == null
                    ? null : response.getProps().getHeaders().get("x-death"));
            Log.info("inspector", order.eventId() + " (" + order.orderId() + ") in '" + Topology.DEAD_LETTER_QUEUE + "'");
            deaths.forEach(death -> Log.info("inspector", "x-death: " + death.describe()));
            channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);   // peek only: put it back
        }
    }
    // endregion
}
