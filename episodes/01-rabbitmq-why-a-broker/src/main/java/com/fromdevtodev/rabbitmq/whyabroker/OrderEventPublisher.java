package com.fromdevtodev.rabbitmq.whyabroker;

import java.io.IOException;

/** Hands the work over to the broker by publishing an "order created" message. */
@FunctionalInterface
public interface OrderEventPublisher {
    void orderCreated(Order order) throws IOException;
}
