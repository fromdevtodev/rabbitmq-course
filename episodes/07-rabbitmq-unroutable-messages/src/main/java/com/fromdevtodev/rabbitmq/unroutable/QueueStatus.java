package com.fromdevtodev.rabbitmq.unroutable;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Prints how many messages are ready in each queue, so you can see which queues received a copy.
 *
 * <p>It uses a <em>passive</em> declare: "tell me about this queue, but do not create it". If the queue does not
 * exist, the broker closes the channel, so each reading uses a fresh one.
 */
final class QueueStatus {
    private QueueStatus() { }

    /** Prints a line every {@code intervalMillis}, {@code samples} times ({@code samples <= 0}: until stopped). */
    static void run(List<String> queues, long samples, long intervalMillis)
            throws IOException, TimeoutException, InterruptedException {
        try (Connection connection = Topology.connect("status")) {
            for (long i = 0; samples <= 0 || i < samples; i++) {
                if (i > 0) {
                    Thread.sleep(intervalMillis);
                }
                Map<String, Long> ready = new LinkedHashMap<>();
                for (String queue : queues) {
                    ready.put(queue, readReady(connection, queue));
                }
                System.out.println(StatusLine.format(LocalTime.now(), ready));
            }
        }
    }

    /** Ready messages of {@code queue}, or {@code null} if the queue does not exist. */
    private static Long readReady(Connection connection, String queue) throws IOException {
        Channel channel = connection.createChannel();
        try {
            return (long) channel.queueDeclarePassive(queue).getMessageCount();
        } catch (IOException queueMissing) {
            return null;
        } finally {
            if (channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException | TimeoutException ignored) {
                    // The reading is already done; a failed close changes nothing.
                }
            }
        }
    }
}
