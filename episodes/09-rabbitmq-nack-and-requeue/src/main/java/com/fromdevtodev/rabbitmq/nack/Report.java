package com.fromdevtodev.rabbitmq.nack;

/** The one line the worker prints for each delivery. Kept apart so the format can be tested. */
final class Report {
    private Report() { }

    /**
     * For example:
     * {@code evt_0002 attempt 3, redelivered=true: order ord_1042 has no email address -> nack, requeue=true}.
     */
    static String line(String eventId, int attempt, boolean redelivered, String detail, Rules.Decision decision) {
        return eventId + " attempt " + attempt + ", redelivered=" + redelivered + ": " + detail
                + " -> " + decision.describe();
    }
}
