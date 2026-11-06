package com.fromdevtodev.rabbitmq.nack;

import java.util.Locale;

/**
 * The names of the story, and our own model of what the worker answers to RabbitMQ.
 *
 * <p>After a delivery the worker must give exactly one answer: an <em>ack</em> (the work is done) or a <em>nack</em>
 * (it was not processed). A nack carries one more choice, {@code requeue}: {@code true} puts the message back in
 * the queue, {@code false} removes it (RabbitMQ discards it, or dead-letters it if the queue is configured to).
 *
 * <p>{@link #decide} is that choice as a pure function, so it can be tested without a broker. The broker is the source
 * of truth for what happens next; the experiments in the README show it.
 */
final class Rules {
    static final String EXCHANGE = "orders";
    static final String QUEUE = "notifications.email";
    static final String BINDING_KEY = "order.created";

    /** What the worker does with a delivery it could not process. */
    enum OnFailure {
        /** Always {@code requeue = true}: the demo of the loop. */
        REQUEUE,
        /** Always {@code requeue = false}: the message leaves the queue. */
        DISCARD,
        /** Requeue only failures that may pass by themselves; never requeue an invalid order. */
        BY_KIND;

        static OnFailure parse(String text) {
            return switch (text.toLowerCase(Locale.ROOT)) {
                case "requeue" -> REQUEUE;
                case "discard" -> DISCARD;
                case "by-kind" -> BY_KIND;
                default -> throw new IllegalArgumentException(
                        "unknown --on-failure '" + text + "' (use requeue, discard or by-kind)");
            };
        }
    }

    /** The worker's answer: an ack, or a nack with its {@code requeue} argument. */
    record Decision(boolean ack, boolean requeue) {
        /** For example {@code ack} or {@code nack, requeue=true}. */
        String describe() {
            return ack ? "ack" : "nack, requeue=" + requeue;
        }
    }

    private Rules() { }

    /** {@code failure == null} means the message was processed. */
    static Decision decide(OnFailure policy, Exception failure) {
        if (failure == null) {
            return new Decision(true, false);
        }
        boolean requeue = switch (policy) {
            case REQUEUE -> true;
            case DISCARD -> false;
            case BY_KIND -> !(failure instanceof EmailWorker.InvalidOrderException);
        };
        return new Decision(false, requeue);
    }
}
