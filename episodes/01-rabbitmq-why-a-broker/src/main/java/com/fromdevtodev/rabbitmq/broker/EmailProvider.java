package com.fromdevtodev.rabbitmq.broker;

/**
 * A stand-in for the external email provider. Nothing is sent: a send is a sleep of {@code latencyMillis}, or, when the
 * provider is {@code down}, an immediate "connection refused".
 *
 * <p>The failure is a refused connection on purpose. A timeout would leave the outcome unknown (the provider may have sent
 * the email), and that is a different lesson.
 */
public record EmailProvider(long latencyMillis, boolean down) {

    // region readme:send
    /** Blocks the calling thread for the provider's latency, or fails at once when the provider is down. */
    public void send(String orderId) throws ProviderUnavailableException {
        if (down) {
            throw new ProviderUnavailableException("email provider unavailable (connection refused)");
        }
        try {
            Thread.sleep(Math.max(0, latencyMillis));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
    // endregion

    public String describe() {
        return down ? "DOWN" : "up, " + latencyMillis + " ms per email";
    }
}
