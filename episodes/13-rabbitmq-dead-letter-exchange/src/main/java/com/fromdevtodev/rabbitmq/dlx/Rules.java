package com.fromdevtodev.rabbitmq.dlx;

/**
 * What the email worker answers to RabbitMQ, written as plain code with no broker in it so it can be read and tested on its own.
 * In this episode an invalid order (no email address) can never succeed, so the answer is always a nack with requeue false:
 * what happens to the message after that is decided by the queue's settings, not by the worker.
 */
final class Rules {
    private Rules() { }

    enum Failure {
        NONE("no failure"),
        INVALID_ORDER("the order has no email address");

        private final String reason;

        Failure(String reason) {
            this.reason = reason;
        }

        String reason() {
            return reason;
        }
    }

    enum Outcome { ACK, NACK_DISCARD }

    static Failure check(OrderEvent order) {
        return order.hasEmailAddress() ? Failure.NONE : Failure.INVALID_ORDER;
    }

    // region readme:decide
    static Outcome decide(Failure failure) {
        return switch (failure) {
            case NONE -> Outcome.ACK;                  // the work is done: acknowledge
            case INVALID_ORDER -> Outcome.NACK_DISCARD; // it never succeeds: nack with requeue false, let the queue's settings decide
        };
    }
    // endregion

    /** The {@code requeue} argument of {@code basicNack}: always false here. */
    static boolean requeue(Outcome outcome) {
        return false;
    }
}
