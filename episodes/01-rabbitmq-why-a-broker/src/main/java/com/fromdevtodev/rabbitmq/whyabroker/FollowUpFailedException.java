package com.fromdevtodev.rabbitmq.whyabroker;

/**
 * The order was saved, but the step after it failed.
 *
 * <p>The caller still has to answer the customer. Note that for a timeout the failure says nothing about
 * whether the other side did the work: the outcome is unknown.
 */
public final class FollowUpFailedException extends Exception {
    private static final long serialVersionUID = 1L;

    private final transient Order order;

    public FollowUpFailedException(Order order, Throwable cause) {
        super("order " + order.id() + " was saved, but the follow-up step failed: " + cause, cause);
        this.order = order;
    }

    public Order order() {
        return order;
    }
}
