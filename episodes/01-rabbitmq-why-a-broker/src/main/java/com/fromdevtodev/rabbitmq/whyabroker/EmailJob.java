package com.fromdevtodev.rabbitmq.whyabroker;

import java.io.IOException;
import java.time.Duration;

/**
 * The consumer's work for one message: send the confirmation email.
 *
 * <p>Deliberately simple. If the provider fails, it waits and tries again <em>for as long as it takes</em>, and
 * only then returns, so the caller can acknowledge the message after the work is done. A real consumer needs a
 * bounded number of retries and a dead-letter queue, and an email that tolerates being sent twice: later episodes.
 */
public final class EmailJob {
    private final EmailClient email;
    private final Duration retryDelay;

    public EmailJob(EmailClient email, Duration retryDelay) {
        this.email = email;
        this.retryDelay = retryDelay;
    }

    public void handle(OrderCreated event) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                email.sendConfirmation(event.toOrder());
                Log.info("email-worker", "email sent for " + event.orderId() + " (event " + event.eventId() + ")");
                return;
            } catch (IOException e) {
                Log.info("email-worker", "could not send email for " + event.orderId() + " (attempt " + attempt + "): "
                        + e.getMessage() + ". The message stays unacknowledged; retrying in " + retryDelay.toMillis() + " ms");
                Thread.sleep(retryDelay);
            }
        }
    }
}
