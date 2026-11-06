package com.fromdevtodev.rabbitmq.queues;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.time.LocalTime;
import java.util.concurrent.TimeoutException;

/**
 * Reads the two numbers this episode is about: how many messages are ready, and how many consumers there are.
 *
 * <p>It uses a <em>passive</em> declare: "tell me about this queue, but do not create it". The reply carries the
 * number of ready messages and the number of consumers. If the queue does not exist, the broker closes the channel,
 * so each reading uses a fresh one.
 */
final class QueueStatus {
    private QueueStatus() { }

    /** Prints a line every {@code intervalMillis}, {@code samples} times ({@code samples <= 0}: until stopped). */
    static void run(long samples, long intervalMillis) throws IOException, TimeoutException, InterruptedException {
        try (Connection connection = Topology.connect("status")) {
            for (long i = 0; samples <= 0 || i < samples; i++) {
                if (i > 0) {
                    Thread.sleep(intervalMillis);
                }
                System.out.println(read(connection));
            }
        }
    }

    private static String read(Connection connection) throws IOException, TimeoutException {
        try (Channel channel = connection.createChannel()) {
            AMQP.Queue.DeclareOk info = channel.queueDeclarePassive(Topology.QUEUE);
            return StatusLine.format(LocalTime.now(), Topology.QUEUE, info.getMessageCount(), info.getConsumerCount());
        } catch (IOException queueMissing) {
            return StatusLine.missing(LocalTime.now(), Topology.QUEUE);
        }
    }

    /** Deletes the queue (and its messages) so a demo starts from nothing. */
    static void delete() throws IOException, TimeoutException {
        try (Connection connection = Topology.connect("reset");
             Channel channel = connection.createChannel()) {
            channel.queueDelete(Topology.QUEUE);
            Log.info("reset", "queue '" + Topology.QUEUE + "' deleted (if it existed)");
        }
    }
}
