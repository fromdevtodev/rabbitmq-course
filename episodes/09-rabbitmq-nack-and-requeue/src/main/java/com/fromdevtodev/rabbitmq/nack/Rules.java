package com.fromdevtodev.rabbitmq.nack;

/**
 * What the worker answers to RabbitMQ for each kind of failure, written as plain code with no broker in it so that it can be read
 * and tested on its own. The worker calls it; the broker never does.
 *
 * <p>The two kinds of failure are the ones of the episode: a PERMANENT failure (the order has no email address: trying again changes
 * nothing) and a TRANSIENT one (the mail server is down for a moment: trying again later may work).
 */
final class Rules {
    private Rules() { }

    /** What went wrong while handling one message. */
    enum Failure {
        NONE("no failure"),
        INVALID_ORDER("the order has no email address"),   // permanent
        MAIL_SERVER_DOWN("the mail server is down");       // transient

        private final String reason;

        Failure(String reason) {
            this.reason = reason;
        }

        String reason() {
            return reason;
        }
    }

    /** The answer the worker sends to RabbitMQ. */
    enum Outcome { ACK, NACK_REQUEUE, NACK_DISCARD }

    /** What the worker does about an INVALID order in a given run: the setting that the two demos change. */
    enum OnInvalid { REQUEUE, DISCARD }

    /** Looks at the message the way the worker does before it sends anything. */
    static Failure check(OrderEvent order) {
        return order.hasEmailAddress() ? Failure.NONE : Failure.INVALID_ORDER;
    }

    // region readme:decide
    /** One answer per kind of failure. */
    static Outcome decide(Failure failure, OnInvalid onInvalid) {
        return switch (failure) {
            case NONE -> Outcome.ACK;                                    // the work is done: acknowledge
            case INVALID_ORDER -> onInvalid == OnInvalid.REQUEUE
                    ? Outcome.NACK_REQUEUE                               // the episode's loop: it fails the same way forever
                    : Outcome.NACK_DISCARD;                              // permanent: do not put it back
            case MAIL_SERVER_DOWN -> Outcome.NACK_REQUEUE;               // transient: it may pass, but requeue does not wait
        };
    }
    // endregion

    /** The {@code requeue} argument of {@code basicNack} that goes with an outcome. */
    static boolean requeue(Outcome outcome) {
        return outcome == Outcome.NACK_REQUEUE;
    }
}
