package com.fromdevtodev.rabbitmq.nack;

/**
 * The business work of the story: "send the order confirmation email". Nothing is really sent; the worker only takes
 * some time ({@code workMillis}) and then either succeeds or fails.
 *
 * <ul>
 *   <li>An order with no email address can never succeed: {@link InvalidOrderException}.
 *   <li>With {@code transientFailures = N}, the first N attempts of every event fail with {@link TransientFailure}
 *       (a mail server that is briefly unavailable) and the next one succeeds.
 * </ul>
 */
final class EmailWorker {
    /** The failure is in the message. Trying again changes nothing. */
    static final class InvalidOrderException extends Exception {
        InvalidOrderException(String message) {
            super(message);
        }
    }

    /** The failure is in the environment. It may pass by itself. */
    static final class TransientFailure extends Exception {
        TransientFailure(String message) {
            super(message);
        }
    }

    private final long workMillis;
    private final int transientFailures;

    EmailWorker(long workMillis, int transientFailures) {
        this.workMillis = workMillis;
        this.transientFailures = transientFailures;
    }

    /** {@code attempt} is 1 for the first delivery of this event in this process, 2 for the next one, and so on. */
    void send(OrderEvent event, int attempt) throws InvalidOrderException, TransientFailure {
        pause();
        if (!event.hasEmail()) {
            throw new InvalidOrderException("order " + event.orderId() + " has no email address");
        }
        if (attempt <= transientFailures) {
            throw new TransientFailure("the mail server is unavailable (simulated)");
        }
    }

    private void pause() {
        if (workMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(workMillis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
