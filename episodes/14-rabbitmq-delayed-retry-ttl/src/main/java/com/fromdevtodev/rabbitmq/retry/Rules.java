package com.fromdevtodev.rabbitmq.retry;

/**
 * What the worker answers to RabbitMQ when the mail server is down, written as plain code with no broker in it so that it can be
 * read and tested on its own. The three answers are the three designs of the episode.
 */
final class Rules {
    private Rules() { }

    /** What went wrong while handling one message. */
    enum Failure {
        NONE("no failure"),
        MAIL_SERVER_DOWN("the mail server is down");   // transient: it will answer again in a few seconds

        private final String reason;

        Failure(String reason) {
            this.reason = reason;
        }

        String reason() {
            return reason;
        }
    }

    /** How the worker reacts to a transient failure: the setting that the demos change. */
    enum OnFailure {
        REQUEUE,       // nack with requeue = true: back in the queue at once
        SLEEP,         // sleep inside the consumer, then requeue
        RETRY_QUEUE    // nack with requeue = false: the queue's dead-letter exchange sends it to the retry queue
    }

    /** The answer the worker sends to RabbitMQ. */
    enum Outcome { ACK, NACK_REQUEUE, SLEEP_THEN_REQUEUE, NACK_DEAD_LETTER }

    // region readme:decide
    /** One answer per setting; a transient failure is the same failure in all three. */
    static Outcome decide(Failure failure, OnFailure onFailure) {
        if (failure == Failure.NONE) {
            return Outcome.ACK;                                       // the work is done: acknowledge
        }
        return switch (onFailure) {
            case REQUEUE -> Outcome.NACK_REQUEUE;                     // back at once: no delay
            case SLEEP -> Outcome.SLEEP_THEN_REQUEUE;                 // the worker waits, holding the message
            case RETRY_QUEUE -> Outcome.NACK_DEAD_LETTER;             // nack without requeue: the policy sends it to the retry queue
        };
    }
    // endregion

    /** The {@code requeue} argument of {@code basicNack} that goes with an outcome. */
    static boolean requeue(Outcome outcome) {
        return outcome == Outcome.NACK_REQUEUE || outcome == Outcome.SLEEP_THEN_REQUEUE;
    }
}
