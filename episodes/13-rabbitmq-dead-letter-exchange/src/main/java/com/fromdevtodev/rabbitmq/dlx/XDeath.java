package com.fromdevtodev.rabbitmq.dlx;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads the {@code x-death} header that RabbitMQ adds to a dead letter (AMQP 0-9-1). The header is a list of entries, the most
 * recent first; each entry says in which queue the message died, why, how many times for that queue and reason, the exchange it was
 * published to and the routing keys it had. Written against plain {@code Map} and {@code List} so it needs no broker to be tested:
 * the client library hands strings over as {@code LongString}, whose {@code toString()} is the text.
 */
final class XDeath {
    private XDeath() { }

    record Death(String queue, String reason, long count, String exchange, List<String> routingKeys) {
        /** One line for the log, e.g. {@code reason=rejected, queue=notifications.email, exchange=orders, count=1, routing-keys=[order.created]}. */
        String describe() {
            return "reason=" + reason + ", queue=" + queue + ", exchange=" + exchange + ", count=" + count
                    + ", routing-keys=" + routingKeys;
        }
    }

    // region readme:xdeath
    static List<Death> parse(Object header) {
        List<Death> deaths = new ArrayList<>();
        if (!(header instanceof List<?> entries)) {
            return deaths;                                  // not dead-lettered: no header
        }
        for (Object entry : entries) {
            if (entry instanceof Map<?, ?> map) {
                List<String> keys = new ArrayList<>();
                if (map.get("routing-keys") instanceof List<?> list) {
                    list.forEach(key -> keys.add(String.valueOf(key)));
                }
                deaths.add(new Death(
                        String.valueOf(map.get("queue")),
                        String.valueOf(map.get("reason")),
                        map.get("count") instanceof Number number ? number.longValue() : 0,
                        String.valueOf(map.get("exchange")),
                        keys));
            }
        }
        return deaths;
    }
    // endregion
}
