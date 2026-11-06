package com.fromdevtodev.rabbitmq.whyabroker;

import java.io.IOException;

/** The "controller" from the video: save the order, run the follow-up, answer. */
public final class OrderService {
    private final OrderRepository orders;
    private final OrderFollowUp followUp;

    public OrderService(OrderRepository orders, OrderFollowUp followUp) {
        this.orders = orders;
        this.followUp = followUp;
    }

    public Order place(String customerId) throws FollowUpFailedException, InterruptedException {
        Order order = orders.save(customerId);
        try {
            followUp.run(order);   // inline: the customer waits here. broker: only a publish.
        } catch (IOException e) {
            throw new FollowUpFailedException(order, e);
        }
        return order;
    }
}
