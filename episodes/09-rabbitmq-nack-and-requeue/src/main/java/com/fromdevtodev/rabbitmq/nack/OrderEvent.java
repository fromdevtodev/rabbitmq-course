package com.fromdevtodev.rabbitmq.nack;

import java.util.Locale;

/**
 * The message used in every demo: an order that needs a confirmation email. The sequence number is baked into the IDs
 * ({@code evt_0001} / {@code ord_1041}, ...) so it is easy to follow one message through the log. The event ID is the identity of
 * the message; a delivery tag never is.
 *
 * <p>A message with no email address is the bad one of the episode: no number of attempts will ever send its email.
 */
public record OrderEvent(String eventId, String orderId, String customerEmail) {

    /** Event number {@code sequence} (1, 2, 3, ...): order {@code 1040 + sequence}, with or without an email address. */
    public static OrderEvent of(long sequence, boolean withoutEmail) {
        long order = 1040 + sequence;
        return new OrderEvent(
                String.format(Locale.ROOT, "evt_%04d", sequence),
                "ord_" + order,
                withoutEmail ? null : "customer-" + order + "@example.com");
    }

    /** A message that can never succeed: the worker has nowhere to send the email. */
    public boolean hasEmailAddress() {
        return customerEmail != null && !customerEmail.isBlank();
    }
}
