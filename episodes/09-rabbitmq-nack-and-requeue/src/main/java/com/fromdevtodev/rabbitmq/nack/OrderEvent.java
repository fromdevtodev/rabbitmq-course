package com.fromdevtodev.rabbitmq.nack;

import java.util.Locale;

/**
 * The message used in every demo: one {@code order.created} event, with the email address the worker must write to.
 * The sequence number is baked into the IDs ({@code evt_0002}, {@code ord_1042}) so that it is easy to follow one
 * message through the log.
 *
 * <p>A message built with {@code withoutEmail} has no email address. That is the poison message of the story: no
 * number of retries will ever give the order an address.
 */
public record OrderEvent(String eventId, String type, String orderId, String customerEmail) {
    public static final String TYPE = "order.created";

    /** Message number {@code sequence} (1, 2, 3, ...). Number 2 becomes {@code evt_0002} for {@code ord_1042}. */
    public static OrderEvent of(long sequence, boolean withoutEmail) {
        return new OrderEvent(
                String.format(Locale.ROOT, "evt_%04d", sequence),
                TYPE,
                String.format(Locale.ROOT, "ord_%04d", 1040 + sequence),
                withoutEmail ? null : String.format(Locale.ROOT, "customer%04d@example.com", sequence));
    }

    boolean hasEmail() {
        return customerEmail != null && !customerEmail.isBlank();
    }
}
