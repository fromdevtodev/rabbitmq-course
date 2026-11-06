package com.fromdevtodev.rabbitmq.whyabroker;

import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The message that travels: a stable event ID, what happened, which order, and enough to find the customer.
 * It matches {@code examples/order-created.json}.
 */
public record OrderCreated(String eventId, String type, String orderId, String customerId) {
    public static final String TYPE = "order.created";

    public static OrderCreated from(Order order) {
        String eventId = String.format(Locale.ROOT, "evt_%08X", ThreadLocalRandom.current().nextInt());
        return new OrderCreated(eventId, TYPE, order.id(), order.customerId());
    }

    public Order toOrder() {
        return new Order(orderId, customerId);
    }
}
