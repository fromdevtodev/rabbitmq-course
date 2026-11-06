package com.fromdevtodev.rabbitmq.whyabroker;

import java.io.IOException;

/**
 * What the order service does after the order is saved and before it answers.
 *
 * <p>This is the only thing that differs between the two designs in the video:
 * {@code emailClient::sendConfirmation} (inline call) or {@code publisher::orderCreated} (publish a message).
 */
@FunctionalInterface
public interface OrderFollowUp {
    void run(Order order) throws IOException;
}
