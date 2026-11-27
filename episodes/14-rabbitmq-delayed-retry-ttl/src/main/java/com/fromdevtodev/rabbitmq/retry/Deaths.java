package com.fromdevtodev.rabbitmq.retry;

import java.util.List;
import java.util.Map;

/**
 * Reads the {@code x-death} header that RabbitMQ adds when it dead-letters a message: a list of entries, one per queue and reason,
 * each with a {@code count}. Here we want one number: how many times the work queue has rejected this message.
 */
final class Deaths {
    private Deaths() { }

    /** How many times {@code queue} dead-lettered the message because it was rejected (0 for a first delivery). */
    static long rejections(Map<String, Object> headers, String queue) {
        if (headers == null || !(headers.get("x-death") instanceof List<?> entries)) {
            return 0;
        }
        long total = 0;
        for (Object entry : entries) {
            if (entry instanceof Map<?, ?> death
                    && queue.equals(String.valueOf(death.get("queue")))
                    && "rejected".equals(String.valueOf(death.get("reason")))
                    && death.get("count") instanceof Number count) {
                total += count.longValue();
            }
        }
        return total;
    }
}
