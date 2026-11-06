package com.fromdevtodev.rabbitmq.whyabroker;

import java.io.IOException;

/** Sends the confirmation email for an order. The real provider is slow and sometimes down. */
@FunctionalInterface
public interface EmailClient {
    void sendConfirmation(Order order) throws IOException;
}
