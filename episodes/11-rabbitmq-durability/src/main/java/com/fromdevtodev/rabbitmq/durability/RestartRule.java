package com.fromdevtodev.rabbitmq.durability;

/**
 * What a <em>graceful</em> broker restart leaves behind, written as code so that you can read it. RabbitMQ implements this
 * itself: the project never calls this class to protect anything. It exists so that you can predict the result and then check
 * your prediction against the real broker ({@code ./run.sh app explain --durable true --persistent false}, then
 * {@code ./run.sh experiment1}).
 *
 * <p>It models a restart only. A crash in the short moment between the broker accepting a message and saving it is a
 * different case, and the rule does not cover it.
 */
final class RestartRule {
    private RestartRule() { }

    /** What is left after the restart. */
    enum Outcome {
        QUEUE_AND_MESSAGES("the queue is back, with its messages"),
        QUEUE_ONLY("the queue is back, empty"),
        NOTHING("nothing is left: the queue is gone, and so is what it held");

        final String description;

        Outcome(String description) {
            this.description = description;
        }
    }

    // region readme:rule
    /** A message needs both settings: a durable queue to come back to, and a persistent message to be saved. */
    static Outcome afterRestart(boolean durableQueue, boolean persistentMessage) {
        if (!durableQueue) {
            return Outcome.NOTHING;
        }
        return persistentMessage ? Outcome.QUEUE_AND_MESSAGES : Outcome.QUEUE_ONLY;
    }
    // endregion
}
