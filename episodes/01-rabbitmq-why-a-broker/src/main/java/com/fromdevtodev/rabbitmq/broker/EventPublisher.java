package com.fromdevtodev.rabbitmq.broker;

import java.io.IOException;

/** What the order service needs from a broker: hand over one event and carry on. */
public interface EventPublisher {
    void publish(OrderEvent event) throws IOException;
}
