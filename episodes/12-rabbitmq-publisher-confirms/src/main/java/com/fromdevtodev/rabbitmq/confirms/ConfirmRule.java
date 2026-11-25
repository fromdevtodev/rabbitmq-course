package com.fromdevtodev.rabbitmq.confirms;

/**
 * What a producer may conclude from each thing the broker can say, written as code so that you can read it. RabbitMQ does not use
 * this class: it exists so that you can predict what an answer means and then check your prediction against the real broker
 * ({@code ./run.sh app explain --answer nack}, then {@code ./run.sh experiment1}).
 *
 * <p>It says what the producer <em>knows</em>. It says nothing about a consumer: a confirm is never about one.
 */
final class ConfirmRule {
    private ConfirmRule() { }

    /** What the producer hears for one message. */
    enum Heard { CONFIRM, NACK, SILENCE }

    /** What it may conclude, and what to do next. */
    enum Verdict {
        TAKEN("the broker took responsibility for the message; no consumer has processed it",
                "stop waiting for this message, and do not assume that anyone has read it"),
        REFUSED("the broker did not take the message",
                "decide: publish it again later, or report the failure; do not treat it as sent"),
        UNKNOWN("the broker may or may not have the message",
                "publish it again with the same event ID, and expect a duplicate");

        final String knowledge;
        final String nextMove;

        Verdict(String knowledge, String nextMove) {
            this.knowledge = knowledge;
            this.nextMove = nextMove;
        }
    }

    // region readme:rule
    /** A confirm is the broker taking responsibility, a nack is the broker refusing, and silence is not an answer. */
    static Verdict whatYouKnow(Heard heard) {
        return switch (heard) {
            case CONFIRM -> Verdict.TAKEN;
            case NACK -> Verdict.REFUSED;
            case SILENCE -> Verdict.UNKNOWN;
        };
    }
    // endregion
}
